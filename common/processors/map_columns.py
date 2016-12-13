"""Map raw field names to fiscal field names where possible.

The mapping is determined by the property `maps_to` in the description file.
Since the general assumption is that all resources in the description file
have the exactly the same data structure, we just grab the mapping for the
first resource.

When building the lookup table, we check that:
    1. The property `maps_to` exists
    2. It's not empty
    2. Its value matches a fiscal field

"""

from logging import info
from datapackage_pipelines.wrapper import ingest, spew
from common.utilities import get_fiscal_field_names, process


def build_lookup_table(datapackage_):
    """Return the mapping for the first resource."""

    fields = datapackage_['resources'][0]['schema']['fields']
    fiscal_fields = get_fiscal_field_names()

    for field in fields:
        checks = ('maps_to' in field,
                  field.get('maps_to'),
                  field.get('maps_to') in fiscal_fields)

        if all(checks):
            yield field['name'], field['maps_to']
            info('%s mapped to %s', field['name'], field['maps_to'])
        else:
            info('Skipping %s', field['name'])


def apply_mapping(row, mapping=None):
    """Apply the mapping to one row."""

    if mapping:
        for old, new in mapping:
            row[new] = row.pop(old)
    return row


if __name__ == '__main__':
    _, datapackage, resources = ingest()
    lookup_table = tuple(build_lookup_table(datapackage))
    new_resources = process(resources, apply_mapping, mapping=lookup_table)
    spew(datapackage, new_resources)
