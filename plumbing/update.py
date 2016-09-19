"""This module detects what has been sourced and bootstraps the pipeline."""

from json import dumps, loads
from logging import debug, info, basicConfig, INFO, DEBUG
from os.path import join, split, isfile
from datapackage.exceptions import ValidationError
from os import walk
from slugify import slugify
from yaml import load, dump

from plumbing.config import (
    PIPELINE_TEMPLATE,
    DATAPACKAGE_FILE,
    PIPELINE_SPECS,
    ROOT_DIR,
    GEOCODE_COLUMN)


basicConfig(level=DEBUG, format='EU-Subsidies: %(message)s')


def create_datapackage(source_description_file):
    """Create a datapackage from the description.source.yaml file."""

    with open(source_description_file) as yaml:
        package = load(yaml.read())

    package['name'] = slugify(package['title'], separator='_')
    package['extraction_processors'] = select_processors(package)

    datapackage_file = source_description_file.replace('yaml', 'json')
    with open(datapackage_file, 'w+') as json:
        json.write(dumps(package, indent=4))

    debug('Created %s', datapackage_file)
    return package


def bootstrap_pipeline(source_folder):
    """Save a pipeline file in the source folder."""

    _, source_name = split(source_folder)
    descriptor = load_source_datapackage()

    with open(PIPELINE_TEMPLATE) as yaml:
        pipeline_template = load(yaml.read())

    pipeline = {source_name: pipeline_template}
    processors = select_processors(descriptor)
    save_processor_placeholders(processors)

    with open(PIPELINE_SPECS, 'w+') as yaml:
        yaml.write(dump(pipeline))

    debug('Saved %s for %s', PIPELINE_SPECS, source_name)


def select_processors(package):
    """Select extraction processors for the pipeline."""

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
    else:
        raise NotImplementedError


def save_processor_placeholders(processors):
    """Drop placeholder processor files in the source folder."""

    for processor in processors:
        if processor in ('scrape_remote_sources', 'scrape_pdf_sources'):
            with open(processor + '.py') as script:
                docstring = "A processor to %s"
                name = processor.replace('_', ' ')
                script.write(docstring % name)
                script.write('\n# Help wanted!')


def load_source_datapackage():
    """Return the contents of the local datapackage file."""

    with open(DATAPACKAGE_FILE) as json:
        return loads(json.read())


def collect_source_folders():
    """Return a generator of source folders."""

    data_folder = join(ROOT_DIR, 'data')
    tree = walk(data_folder)

    valid_geocodes = load_geocodes()

    debug('Collecting sources in %s', data_folder)

    for root, folders, _ in tree:
        for folder in folders:
            _, name = split(folder)
            geocode = name.split('.')[0]
            if geocode:
                if geocode in valid_geocodes:
                    debug('Collected %s', folder)
                    yield join(root, folder)


def load_geocodes():
    """Return a list of valid NUTS codes."""

    geocodes = []
    geocodes_file = join(ROOT_DIR, 'geography', 'geocodes.nuts.csv')

    with open(geocodes_file) as csv:
        lines = csv.readlines()

    for line in lines[2:]:
        geocode = line.split(',')[GEOCODE_COLUMN].strip('"')
        geocodes.append(geocode)

    debug('List of NUTS geocodes: %s', geocodes)
    return tuple(geocodes)


def bootstrap_all_pipelines():
    """Create a pipeline wherever a description file is found."""

    for source_folder in collect_source_folders():
        description_file = join(source_folder, 'description.source.yaml')
        if isfile(description_file):
            debug('Updating the %s pipeline', split(source_folder)[-1])
            package = create_datapackage(description_file)
            bootstrap_pipeline(source_folder)
            save_processor_placeholders(package)
            info('Pipeline ')


if __name__ == '__main__':
    bootstrap_all_pipelines()
