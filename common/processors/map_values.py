"""A processor to map values."""

from logging import warning
from common.utilities import process
from datapackage_pipelines.wrapper import ingest, spew


def map_aliases(row, lookup_tables):
    """Map aliases using the lookup tables provided in the specs."""

    for key, lookup in lookup_tables.items():
        if row[key] in lookup:
            row[key] = lookup[row[key]]
        else:
            warning('%s mapped to None because no alias was found', row[key])
            row[key] = None
    return row


def build_lookup_tables(mappings):
    """Build the lookup tables."""

    def lookup_table(mapping):
        for key, aliases in mapping.items():
            for alias in aliases:
                yield alias, key

    return {
        mapping['field']:
            dict(lookup_table(mapping['mapping']))
        for mapping in mappings
        }


if __name__ == '__main__':
    parameters, _, resources = ingest()
    lookup_tables_ = build_lookup_tables(parameters['mappings'])
    new_resources = process(resources, map_aliases,
                            lookup_tables=lookup_tables_)
    spew(_, new_resources)
