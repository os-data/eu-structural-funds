"""A place for useful functions and classes that don't have a home."""

import json
import yaml
import logging
import os

from datapackage import DataPackage
from .config import (
    CODELISTS_DIR,
    FISCAL_SCHEMA_FILE,
    FISCAL_DATAPACKAGE_FILE,
    FISCAL_MODEL_FILE,
    FEEDBACK_FILE
)


def get_all_codelists():
    """Return all codelists as a dictionary of dictionaries."""

    codelists = {}

    for codelist_file in os.listdir(CODELISTS_DIR):
        codelist_name, _ = os.path.splitext(codelist_file)
        codelist = get_codelist(codelist_name)
        codelists.update({codelist_name: codelist})

    return codelists


def get_codelist(codelist_file):
    """Return one codelist as a dictionary."""

    filepath = os.path.join(CODELISTS_DIR, codelist_file + '.yaml')
    with open(filepath) as stream:
        text = stream.read()
        return yaml.load(text)


def get_fiscal_datapackage(skip_validation=False):
    """Create the master fiscal datapackage from parts."""

    with open(FISCAL_DATAPACKAGE_FILE) as stream:
        text = stream.read()
        datapackage = yaml.load(text)

    with open(FISCAL_SCHEMA_FILE) as stream:
        text = stream.read()
        datapackage['resources'][0]['schema'] = yaml.load(text)

    with open(FISCAL_MODEL_FILE) as stream:
        text = stream.read()
        datapackage['model'] = yaml.load(text)

    if not skip_validation:
        DataPackage(datapackage, schema='fiscal').validate()

    return datapackage


def get_fiscal_fields():
    """Return the fiscal datapackage fields. """

    with open(FISCAL_SCHEMA_FILE) as stream:
        schema = yaml.load(stream.read())

    fields = [field_['name'] for field_ in schema['fields']]
    logging.info('Valid fiscal fields = %s', fields)

    return fields


def write_feedback(section, messages, folder=os.getcwd()):
    """Append messages to the feedback file."""

    filepath = os.path.join(folder, FEEDBACK_FILE)

    with open(filepath) as stream:
        feedback = json.load(stream)

    if section not in feedback:
        feedback[section] = []

    for message in messages:
        feedback[section].append(message)
        logging.warning('[%s] %s', section, message)

    with open(filepath, 'w+') as stream:
        json.dump(feedback, stream, indent=4)


def process(resources, row_processor, **parameters):
    """Apply a row processor to each row of each datapackage resource."""

    # TODO: Put the `process` function inside the pipeline-framework

    logging.info('Parameters = %s', parameters)

    for resource in resources:
        def process_rows(resource_):
            for row in resource_:
                yield row_processor(row, **parameters)

        yield process_rows(resource)
