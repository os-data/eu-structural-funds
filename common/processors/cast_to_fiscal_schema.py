"""This processor casts all fields to their fiscal schema type."""

import logging
import arrow
import yaml
import json

from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.wrapper import spew
from common.config import FISCAL_SCHEMA_FILE
from common.utilities import process


def get_fiscal_types():
    """Return the fiscal datapackage fields."""

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
        yield field_['name'], converters[field_['type']]


converter = dict(get_fiscal_types())
dump = {k: v.__name__ for k, v in converter.items()}
logging.debug('Fiscal type casting: \n%s', json.dumps(dump, indent=4))


def cast_values(row):
    """Cast values to fiscal types."""

    for key, value in row.items():
        if value:
            try:
                if value is None or (type(value) is str and len(value.strip())==0):
                    row[key] = None
                else:
                    row[key] = converter[key](value)
            except (ValueError, arrow.parser.ParserError):
                message = 'Could not cast %s = %s to %s, returning None' % (key, row[key], converter[key])
                logging.warning(message)
                assert False, message

    return row


if __name__ == '__main__':
    _, datapackage, resources = ingest()
    new_resources = process(resources, cast_values)
    spew(datapackage, new_resources)
