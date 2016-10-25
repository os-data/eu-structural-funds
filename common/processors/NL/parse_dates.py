"""This processor parses the date field."""

import logging
import arrow

from arrow.parser import ParserError
from datapackage_pipelines.wrapper import ingest, spew
from common.utilities import process


def parse_currencies(row):
    """Clean up and convert currency fields to floats."""

    date_columns = (
        'Datum van laatste bijwerking',
        'Einddatum',
        'Begindatum'
    )
    for key in date_columns:
        try:
            row[key] = arrow.get(row[key], 'DD.MM.YYYY HH:mm')
        except ParserError:
            if row[key] != '0000-00-00 00:00:00':
                message = 'Could not parse %s to a date, returning None'
                logging.warning(message, row[key])

            row[key] = None

    return row


if __name__ == '__main__':
    parameters, datapackage_, resources = ingest()
    new_resources_ = process(resources, parse_currencies)
    spew(datapackage_, new_resources_)
