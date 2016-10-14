"""A processor to fill in fields which have constant values."""

from datapackage_pipelines.wrapper import ingest, spew
from common.utilities import process


def fill_columns(row, constants=None):
    """Fill columns whose value is constant."""

    if constants:
        for key, constant_value in constants.items():
            row[key] = constant_value
    return row


if __name__ == '__main__':
    parameters, datapackage, resources = ingest()
    new_resources = process(resources, fill_columns, **parameters)
    spew(datapackage, new_resources)
