"""This processor parses currency fields and returns floats."""

from datapackage_pipelines.wrapper import ingest, spew
from common.utilities import process


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
                row[key] = float(row[key]
                                 .replace(characters['currency'], '')
                                 .replace(characters['grouping'], '')
                                 .replace(characters['decimal'], '.')
                                 .strip())

    return row


if __name__ == '__main__':
    parameters, datapackage_, resources = ingest()
    new_resources_ = process(resources, parse_currencies, **parameters)
    spew(datapackage_, new_resources_)
