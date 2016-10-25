"""A processor to convert the description file into a datapackage.

The conversion
    - 1) adds `name` fields in the metadata and in each resource
    - 2) duplicates fields from the first resource to other resources if they
         are missing (so that the description file can stay "dry")

"""

import json
import logging
import yaml

from slugify import slugify
from datapackage_pipelines.wrapper import spew, ingest
from common.config import SOURCE_FILE, DATAPACKAGE_FILE


def remove_empty_properties(properties):
    """Remove empty properties because they cause the validation to fail."""
    return {
        key: value
        for key, value
        in properties.items()
        if value
        }


def create_datapackage(description):
    """Generate a python object from the source description file."""

    description = remove_empty_properties(description)
    description['name'] = slugify(description['title'], separator='-').lower()
    first_resource = description['resources'][0]

    for resource in description['resources']:
        # resource = remove_empty_properties(resource)

        for property_ in first_resource.keys():
            if property_ not in resource:
                resource[property_] = first_resource[property_]

        for i, field in enumerate(resource['schema']['fields']):
            resource['schema']['fields'][i] = remove_empty_properties(field)
            resource['schema']['fields'][i]['type'] = 'string'

        resource['name'] = slugify(resource['title'], separator='-').lower()

    return description


def load_description_file():
    with open(SOURCE_FILE) as stream:
        return yaml.load(stream.read())


def save_datapackage_file(description):
    with open(DATAPACKAGE_FILE, 'w+') as stream:
        stream.write(json.dumps(description, indent=4))


if __name__ == '__main__':
    parameters, _, _ = ingest()
    save_ = parameters.get('save_datapackage')
    description_ = load_description_file()
    datapackage = create_datapackage(description_)
    if save_:
        save_datapackage_file(description_)
    logging.debug('Datapackage: %s', datapackage)
    spew(datapackage, [])
