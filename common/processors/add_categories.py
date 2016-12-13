"""A processor to inject categories values into the data."""

from datapackage_pipelines.wrapper import ingest, spew
from common.row_processor import process


def inject_categories(row, **category_tables):
    """Inject categories values into the data.

    Constants are taken from the following places (in order of override):

        * The processor's pipeline parameters
        * The datapackage's `constant_fields_injector` property
        * Each resource's `constant_fields_injector` property

    If no constants are found, the processor does nothing.

    :param row: one row of data as `dict`
    :param category_tables: a `dict` of lookup tables

    :returns row: the new row

    """

    report = '_pass'

    for category, table in category_tables.items():
        try:
            row[category] = table[row[category]]
        except KeyError:
            report = row[category]

    return row, report


def process_stats(stats):
    """Return a list of unmapped categories."""
    return {'missing_categories_values': stats}


if __name__ == '__main__':
    """Ingest, process and spew out."""

    parameters_, datapackage_, resources_ = ingest()
    new_resources_, stats_ = process(resources_,
                                     inject_categories,
                                     datapackage=datapackage_,
                                     parameters=parameters_)
    spew(datapackage_, new_resources_, stats=process_stats(stats_))
