"""Rename fields according to the 'maps_to` property in the source file.

Assumptions
-----------

The processor assumes that each  `maps_to` property is either:

    1. empty or missing
    2. tagged '_ignored'
    3. valid fiscal field name
    4. a list of valid field name

If it's empty or missing, a warning is issued. If it's none of the above,
an `AssertionError` is raised. Empty or missing fields require more research.
Fields tagged as '_ignored' have been double-checked and deemed irrelevant.

Data processing
---------------

If `maps_to` is a valid fiscal field name, the processor renames the keys in
the data. If `maps_to` is missing, empty or '_ignored', the field is dropped.
If a list is found, the value is passed to each field in the list.

Datapackage mutation
--------------------

The datapackage is updated to match the changes in the data.

"""

from copy import copy
from datapackage_pipelines.wrapper import ingest, spew

from common.utilities import FISCAL_KEYS, process, get_field


def build_mapping_tables(datapackage):
    """Return one lookup table per resource."""

    mappings = []

    for resource in datapackage['resources']:
        mapping = {}

        for field in resource['schema']['fields']:
            if 'maps_to' not in field:
                field['maps_to'] = None
            check_mapping_target(field['maps_to'])
            mapping.update({field['name']: field['maps_to']})

        mappings.append(mapping)

    return mappings


def check_mapping_target(value):
    """Check the mapping target."""

    message = ('{target} is neither '
               'a valid fiscal field, '
               'a list of valid fiscal fields '
               'or an "_ignored" tag').format(target=value)

    valid_cases = [
        not value,
        value == '_ignored',
        value in FISCAL_KEYS,
        isinstance(value, list) and
        all(field_key in FISCAL_KEYS for field_key in value)
    ]

    assert any(valid_cases), message


def update_datapackage(datapackage, mappings):
    """Update the field names and delete the `maps_to` properties."""

    # Datapackage mutation is deliberately kept separate to avoid
    # writing functions with side-effects and facilitate unit-tests.

    for i, mapping in enumerate(mappings):
        fields = datapackage['resources'][i]['schema']['fields']

        for raw_key, fiscal_keys in mapping.items():
            j = get_field(raw_key, fields, index=True)

            fields[j]['mapped_from'] = raw_key
            del fields[j]['maps_to']

            if fiscal_keys and fiscal_keys != '_ignored':
                if not isinstance(fiscal_keys, list):
                    fiscal_keys = [fiscal_keys]

                for fiscal_key in fiscal_keys:
                    new_field = copy(fields[j])
                    new_field.update(name=fiscal_key)
                    fields.append(new_field)

            del fields[j]

    return datapackage


def apply_mapping(row, mappings=None, resource_index=None):
    """Rename data keys with a valid mapping and drop the rest."""

    for raw_key, fiscal_keys in mappings[resource_index].items():
        if not fiscal_keys or fiscal_keys == '_ignored':
            del row[raw_key]
        else:
            if not isinstance(fiscal_keys, list):
                fiscal_keys = [fiscal_keys]
            for fiscal_key in fiscal_keys:
                row[fiscal_key] = row[raw_key]
            row.pop(raw_key)

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
