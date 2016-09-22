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
        3) creates a pipeline file with the appropriate processors
        4) adds placeholder for the processors

    Usage
    -----

    This script supports python3 only: python3 bootstrap.py

"""

import re
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
)


logging.basicConfig(level=logging.DEBUG, format=('[%(levelname)s] '
                                                 '[%(funcName)s] '
                                                 '%(message)s'))


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


def create_datapackage(description_file, source_folder):
    """Convert description.source.yaml into datapackage.json."""

    datapackage_file = os.path.join(source_folder, DATAPACKAGE_FILE)

    with open(description_file) as stream:
        try:
            package = yaml.load(stream.read())
            package['name'] = slugify(package['title'], separator='_')
            package['extraction_processors'] = select_processors(package)
        except (ParserError, ScannerError) as error:
            message = clean_error_message(str(error))
            package = {'yaml_error': message}
            logging.debug('Parser error in %s: %s', description_file, message)

    with open(datapackage_file, 'w+') as stream:
        stream.write(json.dumps(package, indent=4))

    pipeline_id = os.path.split(source_folder)[-1]
    logging.info('%s: created datapackage', pipeline_id)

    return package


def bootstrap_pipeline(package, source_folder):
    """Save the pipeline file inside the source folder."""

    _, source_name = os.path.split(source_folder)
    pipeline = {source_name: DEFAULT_PIPELINE}

    if 'yaml_error' not in package:
        processors = select_processors(package)
        save_processor_placeholders(processors, source_folder)
        save_pipeline(pipeline, source_folder)


def save_pipeline(pipeline, source_folder):
    filepath = os.path.join(source_folder, PIPELINE_FILE)
    with open(filepath, 'w+') as stream:
        stream.write(yaml.dump(pipeline))
    pipeline_id = os.path.split(source_folder)[-1]
    logging.info('%s: created pipeline specs', pipeline_id)


def select_processors(package):
    """Select the appropriate processors for the pipeline."""

    for resource in package['resources']:
        mode = resource['extraction_mode']

        if sum(map(int, mode.values())) > 1:
            message = '{}: extraction mode is ambiguous'
            raise ValueError(message.format(package['name']))

        if any([mode['web_scraper'],
               mode['pdf_scraper'],
               mode['user_interface']]):
            return ['scraper']
        else:
            return []


def save_processor_placeholders(processors, source_folder):
    """Drop placeholders for processors inside the source folder."""

    for processor in processors:
        if processor in ('scraper', 'scrape_pdf_sources'):
            processor_file = os.path.join(source_folder, processor + '.py')
            if not os.path.isfile(processor_file):
                with open(processor_file + '.py', 'w') as script:
                    docstring = '"""{} processor module: help wanted!"""'
                    name = processor.replace('_', ' ').title()
                    script.write(docstring.format(name))


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
                yield os.path.join(root, folder)


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


def bootstrap_all_pipelines():
    """Bootstrap the data pipeline where source descriptions are found."""

    for source_folder in collect_source_folders():
        description_file = os.path.join(source_folder, DESCRIPTION_FILE)
        if os.path.isfile(description_file):
            package = create_datapackage(description_file, source_folder)
            pipeline_id = os.path.split(source_folder)[-1]

            bootstrap_pipeline(package, source_folder)
            logging.info('%s: updated pipeline', pipeline_id)


if __name__ == '__main__':
    bootstrap_all_pipelines()
