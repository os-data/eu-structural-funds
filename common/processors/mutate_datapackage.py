"""This processor modifies the datapackage without modifying the resources."""

from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.wrapper import spew
from os.path import splitext

from common.utilities import process


def set_extension_to_csv(package):
    """Rename the data file with a CSV extension."""

    for resource in package['resources']:
        _, extension = splitext(resource['path'])
        resource['path'] = resource['path'].replace(extension, '.csv')
    return package


if __name__ == '__main__':
    _, datapackage, resources = ingest()
    new_datapackage = set_extension_to_csv(datapackage)
    new_resources = process(resources, lambda x: x)
    spew(new_datapackage, new_resources)
