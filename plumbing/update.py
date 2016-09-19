"""This module detects what has been sourced and bootstraps the pipeline."""


from collections import defaultdict
from glob import glob
from json import dumps, loads
from os.path import join, dirname, abspath, split, isfile
from yaml import load
from slugify import slugify


ROOT_DIR = abspath(join('..', dirname(__file__)))


def make_datapackage(description_file):
    """Produce a datapackage from the description.source.yaml file."""

    with open(description_file) as yaml:
        descriptor = load(yaml.read())

    descriptor['name'] = slugify(descriptor['title'], separator='_')

    datapackage_file = description_file.replace('yaml', 'json')
    with open(datapackage_file, 'w+') as json:
        json.write(dumps(descriptor, indent=4))

    return descriptor


def make_pipeline(source_folder):
    """Drop a pipeline blueprint in the source directory."""

    _, source_name = split(source_folder)

    datapackage_file = join(source_folder, 'description.source.json')
    with open(datapackage_file) as json:
        package = loads(json.read())

    specs = defaultdict(lambda: None)
    specs[source_name] = {
        'schedule': {
            'crontab': None
        },
        'pipeline': {
            'run': 'validate_source_description',
            'parameters': package
        }
    }


def collect_source_folders():
    """Detect national or regional directories."""

    source_folders = []
    tree = glob('*', recursive=True)
    geocodes = load_geocodes()

    for node in tree:
        _, folder_name = split(node)
        if folder_name.split('.')[0] in geocodes:
            source_folders.append(folder_name)

    return source_folders


def load_geocodes():
    """Return a list of valid NUTS codes."""

    geocodes = []
    geocodes_file = join(ROOT_DIR, 'geography', 'geocodes.nuts.csv')

    with open(geocodes_file) as csv:
        lines = csv.readlines()

    for line in lines:
        geocode = line.split(',')[-1].strip('"')
        geocodes.append(geocode)

    return geocodes


def register_datapackage_pipelines():
    """Register a pipeline wherever a description file is found."""

    for source_folder in collect_source_folders():
        description_file = join(source_folder, 'description.source.yaml')
        if isfile(description_file):
            make_datapackage(description_file)
            make_pipeline(source_folder)


if __name__ == '__main__':
    register_datapackage_pipelines()
