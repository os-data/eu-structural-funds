"""This script bootstraps the pipeline.

    Contributors describe the data sources by filling up a pseudo-datapackage
    file named source.description.yaml and dropping it into the national or
    regional source folder. This (simplified) YAML format is chosen over the
    JSON datapackage format because its syntax is easier for non-technical
    people.

    The module recursively walks through the data directory and for each folder
    that matches a valid NUTS geocode:

        1) looks for the source description file
        2) converts it into a datapackage
        3) creates a pipeline file with the appropriate processors
        4) adds placeholder for the processors

    Usage:
        python3 bootstrap.py

"""

import re
import yaml
import json
import os.path
import logging
import csv

from slugify import slugify
from datapackage.exceptions import ValidationError
from yaml.parser import ParserError

from plumbing.config import (
    DATAPACKAGE_FILE,
    PIPELINE_SPECS,
    ROOT_DIR,
    DESCRIPTION_FILE,
    GEOCODES_FILE
)


logging.basicConfig(level=logging.DEBUG, format='Bootstrap: %(message)s')


DEFAULT_PIPELINE = {
    'schedule': {
        'crontab': None,
    },
    'pipeline': {
        'run': 'validate_datapackage',
        'params': 'datapackage.json'
    }
}


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


def create_datapackage(description_file):
    """Convert description.source.yaml into datapackage.json."""

    datapackage_file = description_file.replace('yaml', 'json')

    with open(description_file) as file:
        try:
            package = yaml.load(file.read())
            package['name'] = slugify(package['title'], separator='_')
            package['extraction_processors'] = select_processors(package)
        except ParserError as error:
            message = clean_error_message(str(error))
            package = {'yaml_error': message}
            logging.debug('Parser error in %s: %s', description_file, message)

    with open(datapackage_file, 'w+') as file:
        file.write(json.dumps(package, indent=4))

    logging.debug('Created %s', datapackage_file)

    return package


def bootstrap_pipeline(source_folder):
    """Save the pipeline file inside the source folder."""

    _, source_name = os.path.split(source_folder)
    package = load_source_datapackage(source_folder)
    pipeline = {source_name: DEFAULT_PIPELINE}

    if 'yaml_error' not in package:
        processors = select_processors(package)
        save_processor_placeholders(processors)

    save_pipeline(pipeline, source_folder)


def save_pipeline(pipeline, source_folder):
    file = os.path.join(source_folder, PIPELINE_SPECS)
    with open(file, 'w+') as file:
        file.write(yaml.dump(pipeline))
    logging.debug('Created %s', file)


def select_processors(package):
    """Select the appropriate processors for the pipeline."""

    mode = package['extraction_mode']

    if sum(map(mode.values(), int)) > 1:
        message = '%s: extraction mode is ambiguous'
        raise ValidationError(message % package['name'])

    if mode['download_link']:
        return ['download_remote_sources']
    elif mode['web_scraper']:
        return ['scrape_remote_sources']
    elif mode['pdf_scraper']:
        return ['download_pdf_sources', 'scrape_pdf_sources']
    elif mode['user_interface']:
        raise NotImplementedError('User interface mode not ready yet.')
    else:
        ValueError('No extraction mode specified in the description file.')


def save_processor_placeholders(processors):
    """Drop placeholders for processors inside the source folder."""

    for processor in processors:
        if processor in ('scrape_remote_sources', 'scrape_pdf_sources'):
            with open(processor + '.py') as script:
                docstring = "A processor to %s"
                name = processor.replace('_', ' ')
                script.write(docstring % name)
                script.write('\n# Help wanted!')


def load_source_datapackage(source_folder):
    """Return the contents of the local datapackage file."""

    datapackage_file = os.path.join(source_folder, DATAPACKAGE_FILE)
    with open(datapackage_file) as file:
        return json.loads(file.read())


def collect_source_folders():
    """Return a generator of source folders."""

    data_folder = os.path.join(ROOT_DIR, 'data')
    valid_geocodes = load_geocodes()

    logging.debug('Collecting sources in %s', data_folder)

    for root, folders, _ in os.walk(data_folder):
        for folder in folders:
            _, name = os.path.split(folder)
            geocode = name.split('.')[0]
            if geocode:
                if geocode in valid_geocodes:
                    logging.debug('Collected %s', folder)
                    yield os.path.join(root, folder)


def load_geocodes():
    """Return a list of valid NUTS codes."""

    geocodes = []

    with open(GEOCODES_FILE) as file:
        lines = csv.DictReader(file)
        for i, line in enumerate(lines):
            # The first line has an empty NUTS-code
            if i > 0:
                geocode = line['NUTS-Code']
                geocodes.append(geocode)

    logging.debug('List of NUTS geocodes: %s', geocodes)
    return tuple(geocodes)


def bootstrap_all_pipelines():
    """Bootstrap the data pipeline where source descriptions are found."""

    for source_folder in collect_source_folders():
        description_file = os.path.join(source_folder, DESCRIPTION_FILE)
        if os.path.isfile(description_file):
            package = create_datapackage(description_file)
            pipeline_id = os.path.split(source_folder)[-1]

            bootstrap_pipeline(source_folder)
            save_processor_placeholders(package)

            logging.debug('Updated %s', pipeline_id)


if __name__ == '__main__':
    bootstrap_all_pipelines()
