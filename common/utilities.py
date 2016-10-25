"""A place for useful functions and classes that don't have a home."""

import json
import yaml
import logging
import os
import csv

from datapackage import DataPackage
from .config import (
    CODELISTS_DIR,
    FISCAL_SCHEMA_FILE,
    FISCAL_DATAPACKAGE_FILE,
    FISCAL_MODEL_FILE,
    STATUS_FILE,
    GEOCODES_FILE
)


def get_nuts_codes():
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


def get_fiscal_datapackage(skip_validation=False, source=None):
    """Create the master fiscal datapackage from parts."""

    with open(FISCAL_DATAPACKAGE_FILE) as stream:
        text = stream.read()
        fiscal_datapackage = yaml.load(text)

    datapackage = source if source else fiscal_datapackage

    with open(FISCAL_SCHEMA_FILE) as stream:
        text = stream.read()
        datapackage['resources'][0]['schema'] = yaml.load(text)
        datapackage['resources'][0].update(mediatype='text/csv')

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
    """Append messages to the status file."""

    filepath = os.path.join(folder, STATUS_FILE)

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
