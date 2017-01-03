"""A processor to inject constant values into the data."""

from datapackage_pipelines.wrapper import ingest, spew
from common.row_processor import process


def inject_constants(row, **constants):
    """Inject constant values into the data.

    Constants are taken from the following places (in order of override):

        * The processor's pipeline parameters
        * The datapackage's `constant_fields_injector` property
        * Each resource's `constant_fields_injector` property

    If no constants are found, the processor does nothing.

    :param row: one row of data as `dict`
    :param constants: a `dict` of constants

    :returns row: the new row

    """

    stats = '_pass'

    for key, value in constants.items():
        if value is not None:
            row[key] = value

    return row, stats


if __name__ == '__main__':
    """Ingest, process and spew out."""

    parameters_, datapackage_, resources_ = ingest()
    new_resources_, _ = process(resources_,
                                inject_constants,
                                datapackage=datapackage_,
                                parameters=parameters_)
    spew(datapackage_, new_resources_)
