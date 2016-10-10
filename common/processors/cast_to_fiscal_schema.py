"""This processor casts all fields to their fiscal schema type."""

import logging
import arrow
import yaml

from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.wrapper import spew
from common.config import FISCAL_SCHEMA_FILE


def cast_values(row, fiscal_types):
    """Cast values to fiscal types.
    """
    for key, value in row.items():
        if value:
            row[key] = fiscal_types[key](row[key])
    return row


def process_resources(resources, fiscal_types):
    """Return an iterator of row iterators.
    """
    for resource in resources:
        def process_rows(resource_):
            for row in resource_:
                yield cast_values(row, fiscal_types)

        yield process_rows(resource)


def get_fiscal_types():
    """Return the fiscal datapackage fields.
    """
    with open(FISCAL_SCHEMA_FILE) as stream:
        schema = yaml.load(stream.read())

    # I wrap arrow over the standard datetime library because it tries
    # to convert strings to datetime without explicitly requiring a format.
    # Also, and most importantly, it doesn't trip over values that have
    # already been cast to datetime (i.e. behaves like int and string).
    # Maybe I'll come up with something better later.

    converters = {
        'date': lambda x: arrow.get(x).date(),
        'string': str,
        'number': float
    }

    for field_ in schema['fields']:
        logging.debug('Casting %s to %s', field_['name'], field_['type'])
        yield field_['name'], converters[field_['type']]


if __name__ == '__main__':
    parameters, datapackage_, resources_ = ingest()
    fiscal_types_ = dict(get_fiscal_types())
    new_resources_ = process_resources(resources_, fiscal_types_)
    spew(datapackage_, new_resources_)
