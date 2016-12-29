"""This processor reshapes the data to match the fiscal schema."""

from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.wrapper import spew
from common.utilities import get_fiscal_field_names
import logging

def process_row(row, fiscal_fields):
    """Add and remove appropriate columns.
    """
    surplus_keys = set(row) - set(fiscal_fields)
    missing_keys = set(fiscal_fields) - set(row)
    for key in missing_keys:
        row[key] = None
    for key in surplus_keys:
        del row[key]
    assert set(row) == set(fiscal_fields)
    return row


def process_resources(resources, fiscal_fields):
    """Return an iterator of row iterators.
    """
    for resource in resources:
        def process_rows(resource_):
            for i, row in enumerate(resource_):
                yield process_row(row, fiscal_fields)

        yield process_rows(resource)


if __name__ == '__main__':
    parameters_, datapackage_, resources_ = ingest()
    for resource in datapackage_['resources']:
        fiscal_fields_ = set(get_fiscal_field_names())
        fields = resource['schema']['fields']
        new_fields = []
        for field in fields:
            if field['name'] in fiscal_fields_:
                new_fields.append(field)
                fiscal_fields_.remove(field['name'])
        for f in fiscal_fields_:
            new_fields.append({
                'name': f,
                'type': 'string'
            })
        resource['schema']['fields'] = new_fields

    fiscal_fields_ = set(get_fiscal_field_names())
    new_resources_ = process_resources(resources_, fiscal_fields_)
    spew(datapackage_, new_resources_)
