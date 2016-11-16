"""This processor parses currency fields and returns floats."""
from logging import warning

from datapackage_pipelines.wrapper import ingest, spew
from common.utilities import process, format_to_json


def parse_currencies(row, fields=None, characters=None):
    """Clean up and convert currency fields to floats."""

    assert fields, 'Missing `fields` parameter'
    assert characters, 'Missing `characters` parameter'

    for key in fields:
        if row[key] is not None:
            row[key] = str(row[key])

            if not row[key].strip():
                row[key] = None
            else:
                try:
                    row[key] = float(row[key]
                                     .replace(characters['currency'], '')
                                     .replace(characters['grouping'], '')
                                     .replace(characters['decimal'], '.')
                                     .strip())
                except ValueError as error:
                    warning('%s in row\n%s', error, format_to_json(row))
    return row


if __name__ == '__main__':
    parameters, datapackage_, resources = ingest()
    new_resources_ = process(resources, parse_currencies, **parameters)
    spew(datapackage_, new_resources_)
