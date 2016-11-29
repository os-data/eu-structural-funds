"""This processor converts excel dates (int) to python datetime objects."""

from xlrd.xldate import xldate_as_datetime
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.wrapper import spew
from logging import warning


# See http://stackoverflow.com/questions/1108428/...
# ...how-do-i-read-a-date-in-excel-format-in-python


def convert_dates(row, date_fields, date_mode):
    """Rename the row keys according to the mapping.
    """
    for date_field in date_fields:
        try:
            row[date_field] = \
                xldate_as_datetime(float(row[date_field]), date_mode).date()
        except ValueError:
            warning('Could not convert excel date = %s', row[date_field])
            row[date_field] = None
    return row


def process_resources(resources, date_fields, date_mode):
    """Return an iterator of row iterators.
    """
    for resource in resources:
        def process_rows(resource_):
            for row in resource_:
                yield convert_dates(row, date_fields, date_mode)

        yield process_rows(resource)


if __name__ == '__main__':
    parameters, datapackage_, resources_ = ingest()
    date_fields_ = parameters['date-fields']
    date_mode_ = parameters['date-mode']
    new_resources_ = process_resources(resources_, date_fields_, date_mode_)
    spew(datapackage_, new_resources_)
