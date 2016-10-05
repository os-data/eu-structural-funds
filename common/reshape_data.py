"""This processor reshapes the data to match the fiscal schema."""

from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.wrapper import spew
from plumbing.utilities import get_fiscal_fields


def process_row(row, fiscal_fields):
    """Add and remove appropriate columns.
    """
    surplus_keys = set(row) - set(fiscal_fields)
    missing_keys = set(fiscal_fields) - set(row)
    for key in surplus_keys:
        del row[key]
    for field in missing_keys:
        row[field] = None
    return row


def process_resources(resources, fiscal_fields):
    """Return an iterator of row iterators.
    """
    for resource in resources:
        def process_rows(resource_):
            for row in resource_:
                yield process_row(row, fiscal_fields)
        yield process_rows(resource)


if __name__ == '__main__':
    parameters_, datapackage_, resources_ = ingest()
    fiscal_fields_ = get_fiscal_fields()
    new_resources_ = process_resources(resources_, fiscal_fields_)
    spew(datapackage_, new_resources_)
