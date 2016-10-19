"""This processor parses the date_field field."""

import re
import arrow

from datetime import date
from datapackage_pipelines.wrapper import ingest, spew
from common.utilities import process

weekdays = {
    'monday',
    'tuesday',
    'wednesday',
    'thursday',
    'friday',
    'saturday',
    'sunday'
}

long_format = 'dddd, MMMM DD, YYYY'
short_format = 'DD/MM/YYYY'

quarter_pattern = re.compile(r'Q[uarter|uarter | ]*(\d),* *(\d{4})')
quarter_month = {1: 1, 2: 4, 3: 7, 4: 10}
quarter_end_day = {1: 31, 2: 30, 3: 31, 4: 31}


def parse_dates(row, date_fields=None):
    """Convert the start date into a python date object."""

    for date_field in date_fields:
        raw_date = row[date_field]
        first_token = raw_date.split(',')[0].lower()

        if first_token in weekdays:
            row[date_field] = arrow.get(raw_date, long_format).date()

        elif '/' in raw_date:
            row[date_field] = arrow.get(raw_date, short_format).date()

        elif 'Q' in raw_date:
            match = quarter_pattern.search(raw_date)

            if match:
                quarter = int(match.group(1))
                year = int(match.group(2))
                month = quarter_month[quarter]
                day = quarter_end_day[quarter] if 'End' in date_field else 1
                row[date_field] = date(year, month, day)

        else:
            year = int(raw_date)
            row[date_field] = date(year, 1, 1)

    return row


if __name__ == '__main__':
    parameters, datapackage, resources = ingest()
    new_resources = process(resources, parse_dates, **parameters)
    spew(datapackage, new_resources)
