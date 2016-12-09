"""A processor to drop, rename, split and merge fields."""

from copy import deepcopy
from collections import Counter
from logging import info

from datapackage_pipelines.wrapper import ingest, spew
from jsontableschema import Schema

from common.utilities import process

UNSUPPORTED_JTS_TYPES = (
    'object',
    'array',
    'geopoint',
    'geojson',
    'any'
)


def map_fields(datapackage, resources, mapper='maps_to', ignore=None):
    """Drop, rename, split and merge fields.

    The mapping is specified by the field's mapping property in the datapackage
    schema. The processor works as follows:

        * To drop field, omit, leave the mapping empty or use ignored values
        * To rename a field, set the mapping to the new name
        * To split a field, set the mapping to a list of names
        * To merge fields, assign the same mapping to multiple fields

    Fields resulting from a merge are stored as JSON Table Schema arrays.

    :param datapackage: resources must have a JSON Table Schema
    :param resources: there are no assumptions about the data

    :param mapper: the field property holding the mapping target
    :type mapper: str

    :param ignore: if the mapping is in the list, the field will be dropped
    :type ignore: list

    :returns datapackage: with its schema updated
    :returns resources: with its fields updated
    :returns stats: the mapping mapping_tables used to process the data

    :raises: AssertionError on bad arguments
    :raises: ValueError if a mapping target is not valid
    :raises: TypeError when trying to merge unsupported JSON Table Schema types

    """

    if ignore is None:
        ignore = []

    assert isinstance(mapper, str), 'mapper argument must be a str'
    assert isinstance(ignore, list), 'ignore argument must be a list'

    ignore.append(None)

    mapping_tables = _build_mapping_tables(datapackage, mapper, ignore)
    updated_datapackage = _update_datapackage(datapackage,
                                              mapping_tables,
                                              ignore)
    row_context = {
        'mappings': mapping_tables,
        'ignore': ignore,
        'pass_resource_index': True
    }
    new_resources = process(resources, _process_row, **row_context)

    info('Mapping tables: %s', mapping_tables)
    info('Updated datapackage: %s', updated_datapackage)

    return (updated_datapackage,
            new_resources,
            mapping_tables)


# Mapping tables
# -----------------------------------------------------------------------------

def _build_mapping_tables(datapackage, mapper, ignore):
    """Return a flat mapping table for each resource."""

    mapping_tables = []

    for resource in datapackage['resources']:
        counter = Counter()
        mapping_table = []

        for field in Schema(resource['schema']).fields:
            for new_name in _get_mapping_targets(field, mapper, ignore):
                mapping_table.append((field.name, new_name))

                counter.update([new_name])
                if counter[new_name] > 0:
                    if field.type in UNSUPPORTED_JTS_TYPES:
                        message = '{} types cannot be merged'
                        raise TypeError(message.format(field.type))

        mapping_tables.append(mapping_table)

    return mapping_tables


def _get_mapping_targets(field, mapper, ignore):
    """Return a list of new names for a given field."""

    mappings = field.descriptor.get(mapper)

    if not isinstance(mappings, list):
        mappings = [mappings]

    for mapping in mappings:
        if mapping in ignore:
            yield
        else:
            if not isinstance(mapping, str):
                message = 'mapping target for {} is not a str: {}'
                raise ValueError(message.format(field.name, mapping))

            yield mapping


# Datapackage update
# -----------------------------------------------------------------------------

def _update_datapackage(datapackage, mapping_tables, ignore):
    """Update or delete fields."""

    resources = datapackage['resources']

    for resource, mapping_table in zip(resources, mapping_tables):
        schema = Schema(resource['schema'])
        new_fields = {}

        for old_name, new_name in mapping_table:
            if new_name not in ignore:

                if new_name not in new_fields:
                    old_field = schema.get_field(old_name)
                    new_field = deepcopy(old_field.descriptor)
                    new_field.update(name=new_name, mapped_from=[old_name])
                    del new_field['maps_to']

                    new_fields.update({new_name: new_field})

                else:
                    new_field = new_fields[new_name]
                    new_field.update(type='array')
                    new_field['mapped_from'].append(old_name)

        resource['schema']['fields'] = list(new_fields.values())

    return datapackage


# Data processing
# -----------------------------------------------------------------------------

def _process_row(row, mappings, ignore, resource_index):
    """Drop, rename, split and merge data fields based on the mapping table."""

    new_row = {}
    counter = Counter()

    for old_key, new_key in mappings[resource_index]:
        if new_key not in ignore:
            value = row[old_key]

            if new_key not in new_row:
                new_row[new_key] = value
            else:
                if counter[new_key] > 1:
                    values = new_row[new_key]
                else:
                    values = [new_row[new_key]]

                values.append(value)
                new_row[new_key] = values

            counter.update([new_key])

    return new_row


if __name__ == '__main__':
    """Ingest, process data, update datapackage and spew out."""

    parameters_, datapackage_, resources_ = ingest()
    updated_datapackage_, new_resources_, stats_ = \
        map_fields(datapackage_, resources_, **parameters_)
    spew(updated_datapackage_, new_resources_, stats=stats_)
