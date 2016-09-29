"""This script bootstraps the pipeline.

    Context
    -------

    Contributors describe the data sources by filling up a pseudo-datapackage
    file named source.description.yaml and dropping it into the national or
    regional source folder. This (simplified) YAML format is chosen over the
    JSON datapackage format because its syntax is easier for non-technical
    people.

    Bootstrap process
    -----------------

    The module recursively walks through the data directory and for each folder
    that matches a valid NUTS geocode:

        1) looks for the source description file
        2) converts it into a datapackage
        3) saves a pipeline-specs file with the appropriate processors
        4) adds placeholder for a scraper if needed

    Usages
    ------

    python3 -m plumbing.bootstrap # bootstrap all pipelines
    python3 -m plumbing.bootstrap FR.france/2014-2020
    python3 -m plumbing.bootstrap AT.austria/AT11.burgenland

    This script only supports python3.

"""

import re
import sys
import yaml
import json
import os.path
import logging
import csv

from slugify import slugify
from yaml.parser import ParserError
from yaml.scanner import ScannerError

from plumbing.config import (
    DATAPACKAGE_FILE,
    PIPELINE_FILE,
    ROOT_DIR,
    DESCRIPTION_FILE,
    GEOCODES_FILE,
    DEFAULT_PIPELINE,
    EXTRACTOR_FILE)


logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s] %(message)s')


def clean_error_message(message):
    """Make the error message more readable."""

    substitutions = (
        ('\n', ''),
        ('\"', ''),
        (' +', ' ')
    )
    for substitution in substitutions:
        message = re.sub(*substitution, message)

    return message


def create_datapackage(source_folder):
    """Convert description.source.yaml into datapackage.json."""

    description_file = os.path.join(source_folder, DESCRIPTION_FILE)
    datapackage_file = os.path.join(source_folder, DATAPACKAGE_FILE)

    with open(description_file) as stream:
        try:
            package = yaml.load(stream.read())
            package['name'] = slugify(package['title'], separator='_')
        except (ParserError, ScannerError) as error:
            message = clean_error_message(str(error))
            package = {'yaml_error': message}
            logging.debug('Parser error in %s: %s', description_file, message)

    with open(datapackage_file, 'w+') as stream:
        stream.write(json.dumps(package, indent=4))

    pipeline_id = os.path.split(source_folder)[-1]
    logging.info('%s: created datapackage', pipeline_id)

    return package


def register_processors(processors, pipeline_id):
    """Register processors according to the datapackage specs."""

    pipeline = {pipeline_id: DEFAULT_PIPELINE}
    pipeline[pipeline_id]['pipeline'] = processors
    return pipeline


def save_pipeline(source_folder, pipeline, pipeline_id):
    """Save the pipeline-specs.yaml file inside the source folder."""

    filepath = os.path.join(source_folder, PIPELINE_FILE)
    with open(filepath, 'w+') as stream:
        stream.write(yaml.dump(pipeline))
    logging.info('%s: created pipeline specs', pipeline_id)


def select_processors(package):
    """Select the appropriate processors for the pipeline with v1."""

    downloader = [
        {'run': 'validate_source', 'parameters': DATAPACKAGE_FILE},
        {'run': 'downloader'}]
    scraper = [
        {'run': 'scraper', 'parameters': DATAPACKAGE_FILE}
    ]

    if 'scraper_required' in package:
        # v3 descriptions
        if package['scraper_required']:
            processors = scraper
        else:
            processors = downloader
    else:
        # v1 descriptions
        mode = package['resources'][0]['extraction_mode']
        mode_sum = sum(map(int, mode.values()))

        if mode_sum in (0, 2, 3):
            message = '{}: extraction mode is ambiguous'
            raise ValueError(message.format(package['name']))

        if mode['download_link']:
            processors = downloader
        else:
            processors = scraper

    processors.append({'run': 'dump',
                       'parameters': {'out-file': 'source.datapackage.zip'}})

    # noinspection PyTypeChecker
    processor_ids = [processor['run'] for processor in processors]
    return processors, processor_ids


def save_scraper_placeholder(source_folder, pipeline_id):
    """Drop placeholders for processors inside the source folder."""

    processor_file = os.path.join(source_folder, EXTRACTOR_FILE)
    if not os.path.exists(processor_file):
        with open(processor_file, 'w+') as stream:
            docstring = '"""Scraping module for {} wanted!"""\n'
            stream.write(docstring.format(pipeline_id))
        logging.debug('%s: help wanted for scraper', pipeline_id)


# noinspection PyAssignmentToLoopOrWithParameter
def collect_source_folders():
    """Return a generator of source folders."""

    data_folder = os.path.join(ROOT_DIR, 'data')
    valid_geocodes = load_geocodes()

    logging.info('Collecting sources in %s', data_folder)

    for root, folders, _ in os.walk(data_folder):
        for folder in folders:
            _, name = os.path.split(folder)
            geocode = name.split('.')[0]

            if geocode in valid_geocodes:
                logging.debug('%s: detected source folder', folder)
                main_folder = os.path.join(root, folder)
                yield main_folder

                for _, sub_folders, _ in os.walk(main_folder):
                    for sub_folder in sub_folders:
                        logging.debug('%s/%s: detected source folder',
                                      folder, sub_folder)
                        yield os.path.join(main_folder, sub_folder)


def load_geocodes():
    """Return a list of valid NUTS codes."""

    geocodes = []

    with open(GEOCODES_FILE) as stream:
        lines = csv.DictReader(stream)
        for i, line in enumerate(lines):
            # The first line has an empty NUTS-code
            if i > 0:
                geocode = line['NUTS-Code']
                geocodes.append(geocode)

    logging.debug('Loaded %d NUTS geocodes', len(geocodes))
    return tuple(geocodes)


def bootstrap_pipelines(pipeline_folder=None):
    """Bootstrap data pipelines where source description files are found."""

    if not pipeline_folder:
        pipeline_folders = collect_source_folders()
    else:
        pipeline_folders = [pipeline_folder]

    for pipeline_folder in pipeline_folders:
        description_file = os.path.join(pipeline_folder, DESCRIPTION_FILE)

        if os.path.exists(description_file):
            pipeline_id = os.path.basename(pipeline_folder)
            package = create_datapackage(pipeline_folder)

            if 'yaml_error' not in package:
                processors, processor_ids = select_processors(package)
                pipeline = register_processors(processors, pipeline_id)
                save_pipeline(pipeline_folder, pipeline, pipeline_id)

                if 'scraper' in processor_ids:
                    save_scraper_placeholder(pipeline_folder, pipeline_id)

                logging.info('%s: updated pipeline', pipeline_id)


def dispatch_command():
    """Parse the command line argument and call bootstrap_pipelines."""

    pipeline_folder = None

    if len(sys.argv) > 2:
        ValueError('Too many command line arguments')

    if len(sys.argv) == 2:
        fullpath = ROOT_DIR, 'data', sys.argv[1]
        pipeline_folder = os.path.abspath(os.path.join(*fullpath))
        if not os.path.exists(pipeline_folder):
            raise FileNotFoundError(pipeline_folder)

    bootstrap_pipelines(pipeline_folder=pipeline_folder)


if __name__ == '__main__':
    dispatch_command()
