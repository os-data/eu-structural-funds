"""Rename fields according to 'maps_to` in the source description.

Assumptions
-----------

The processor assumes that each `maps_to` property is:

    1. a valid fiscal field name
    2. tagged '_unknown'
    3. tagged '_ignored'

If it's empty, the processor toggles it to '_unknown' and a warning is issued.
In any other case an `AssertionError` is raised. The convention is that fields
tagged as '_ignored' have been deemed irrelevant and those tagged as '_unknown'
require some more research.

Processing
----------

If `maps_to` is a valid fiscal field name, the processor renames the keys in
the data and updates the datapackage accordingly. If `maps_to` is '_unknown`
or `_ignored`, the field is dropped altogether from the data and the
datapackage.


"""

from datapackage_pipelines.wrapper import ingest, spew
from common.utilities import get_fiscal_field_names, process


def build_mapping_tables(datapackage):
    """Return one mapping table per resource."""

    fiscal_field_names = get_fiscal_field_names()

    mappings = []

    for resource in datapackage['resources']:
        mapping = {}

        for field in resource['schema']['fields']:
            if not field.get('maps_to'):
                field['maps_to'] = '_unknown'

            message = ('{} is neither a valid fiscal field, '
                       '_unknown or _ignore')

            assert any([
                field['maps_to'] == '_unknown',
                field['maps_to'] == '_ignored',
                field['maps_to'] in fiscal_field_names
            ]), message.format(field['maps_to'])

            mapping.update({field['name']: field['maps_to']})

        mappings.append(mapping)

    return mappings


def update_datapackage(datapackage, mappings):
    """Update the field names and delete the `maps_to` properties."""

    for i, resource in enumerate(datapackage['resources']):
        fields = []

        for field in resource['schema']['fields']:
            fiscal_key = mappings[i][field['name']]

            if fiscal_key not in ('_unknown', '_ignored'):
                field.update({'name': fiscal_key})
                del field['maps_to']
                if 'translates_to' in field:
                    del field['translates_to']
                fields.append(field)

        resource['schema']['fields'] = fields

    return datapackage


def apply_mapping(row, mappings=None, resource_index=None):
    """Rename data keys with a valid mapping and drop the rest."""

    for raw_key, fiscal_key in mappings[resource_index].items():
        if fiscal_key in ('_ignored', '_unknown'):
            del row[raw_key]
        else:
            row[fiscal_key] = row.pop(raw_key)

    return row


if __name__ == '__main__':
    _, datapackage_, resources_ = ingest()
    mappings_ = build_mapping_tables(datapackage_)
    datapackage_ = update_datapackage(datapackage_, mappings_)
    new_resources_ = process(resources_,
                             apply_mapping,
                             mappings=mappings_,
                             pass_resource_index=True)
    spew(datapackage_, new_resources_)
