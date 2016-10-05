"""Map the raw columns names to fiscal fields where possible."""

import logging
from datapackage_pipelines.wrapper import ingest, spew
from plumbing.utilities import get_fiscal_fields


def get_column_mappings(datapackage, fiscal_fields):
    """Return a lookup table matching old to new field names.
    """
    for resource in datapackage['resources']:
        mapping = {
            field['name']: field['maps_to']
            for field in resource['schema']['fields']
            if field['maps_to'] in fiscal_fields
        }
        logging.info('Resource = %s', resource['name'])
        logging.info('Column map = %s', mapping)
        yield mapping


def process_row(row, mapping):
    """Rename the row keys according to the mapping.
    """
    for old, new in mapping.items():
        row[new] = row.pop(old)
    return row


def process_resources(resources, mappings):
    """Return an iterator of row iterators.
    """
    for resource, mapping in zip(resources, mappings):
        def process_rows(resource_):
            for row in resource_:
                yield process_row(row, mapping)
        yield process_rows(resource)


if __name__ == '__main__':
    _, datapackage_, resources_ = ingest()
    fiscal_fields_ = get_fiscal_fields()
    mappings_ = get_column_mappings(datapackage_, fiscal_fields_)
    new_resources_ = process_resources(resources_, mappings_)
    spew(datapackage_, new_resources_)
