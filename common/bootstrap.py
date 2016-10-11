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

    Unless a source folder is passed as an arguments, the module recursively
    walks through the data directory and for each folder that matches a
    valid NUTS geocode:

        1) detects sub-folders and in each one...
        2) looks for the source description file
        3) saves a simple pipeline-specs.yaml
        4) adds placeholder for a scraper if needed

    Usages
    ------

    python3 -m common.bootstrap # bootstrap all pipelines
    python3 -m common.bootstrap FR.france/2014-2020
    python3 -m common.bootstrap AT.austria/AT11.burgenland

    This script only supports python3.

"""

import csv
import json
import logging
import os.path
import sys
import yaml

from yaml.scanner import ScannerError
from yaml.parser import ParserError
from common.utilities import write_feedback
from common.config import (
    PIPELINE_FILE,
    ROOT_DIR,
    DESCRIPTION_FILE,
    GEOCODES_FILE,
    DEFAULT_PIPELINE,
    EXTRACTOR_FILE,
    FEEDBACK_FILE
)

sys.path.append(ROOT_DIR)
logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s] %(message)s')


def create_feedback_file(pipeline_id, source_folder):
    """Save a feedback file for this source."""

    filepath = os.path.join(source_folder, FEEDBACK_FILE)
    if not os.path.exists(filepath):
        with open(filepath, 'w+') as stream:
            json.dump({}, stream)
        logging.info('%s: created %s', pipeline_id, FEEDBACK_FILE)
    else:
        logging.info('%s: %s already exists', pipeline_id, FEEDBACK_FILE)


def initialize_pipeline(source_folder, pipeline_id):
    """Save the pipeline-specs.yaml file inside the source folder."""

    filepath = os.path.join(source_folder, PIPELINE_FILE)
    if not os.path.exists(filepath):
        pipeline = {pipeline_id: DEFAULT_PIPELINE}
        with open(filepath, 'w') as stream:
            stream.write(yaml.dump(pipeline))
        logging.info('%s: created %s', pipeline_id, PIPELINE_FILE)
    else:
        logging.info('%s: %s already exists', pipeline_id, PIPELINE_FILE)


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

            if geocode and geocode in valid_geocodes:
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

    with open(GEOCODES_FILE) as stream:
        lines = csv.DictReader(stream)
        geocodes = []
        for i, line in enumerate(lines):
            # The first line has an empty NUTS-code
            if i > 0:
                geocode = line['NUTS-Code']
                geocodes.append(geocode)

    logging.debug('Loaded %d NUTS geocodes', len(geocodes))
    return tuple(geocodes)


def load_description_file(description_file):
    """Load the source description file or give feedback upon failure."""

    with open(description_file) as stream:
        try:
            return yaml.load(stream.read())
        except (ScannerError, ParserError) as error:
            source_folder = os.path.dirname(description_file)
            section = 'syntax error in {}'.format(DESCRIPTION_FILE)
            write_feedback(section, [str(error)], folder=source_folder)


def bootstrap_pipelines(pipeline_folders=None):
    """Bootstrap data pipelines where source description files are found."""

    pipeline_folders = pipeline_folders or collect_source_folders()

    for pipeline_folder in pipeline_folders:
        description_file = os.path.join(pipeline_folder, DESCRIPTION_FILE)
        pipeline_id = os.path.basename(pipeline_folder)

        if os.path.exists(description_file):
            create_feedback_file(pipeline_id, pipeline_folder)
            description = load_description_file(description_file)

            if description:
                initialize_pipeline(pipeline_folder, pipeline_id)

                if 'scraper_required' in description:
                    save_scraper_placeholder(pipeline_folder, pipeline_id)

                logging.info('%s: bootstrap DONE', pipeline_id)


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

    bootstrap_pipelines(pipeline_folders=[pipeline_folder])


if __name__ == '__main__':
    dispatch_command()
