"""Test module for the `map_columns` processor."""

from copy import deepcopy
from pytest import fixture
from common.processors.map_columns import build_lookup_table, apply_mapping


@fixture(scope='function')
def lookup_table():
    """The lookup table is a generator so we need a new one for each test.
    """
    datapackage = {
        'resources': [
            {
                'schema': {
                    'fields': [
                        {
                            'name': 'foo',
                            'maps_to': 'project_id'
                        },
                        {
                            'name': 'bar',
                            'maps_to': 'invalid_fiscal_field'
                        },
                        {
                            'name': 'spam',
                            'maps_to': None
                        },
                        {
                            'name': 'eggs',
                        },

                    ]

                }
            }
        ]
    }
    return build_lookup_table(datapackage)


@fixture(scope='function')
def input_row():
    """Row objects are mutable so we need a new one for each test.
    """
    return {
        'foo': 1,
        'bar': 2,
        'spam': 3,
        'eggs': 4
    }


# noinspection PyShadowingNames
def test_build_lookup_table_returns_correct_mapping(lookup_table):
    assert dict(lookup_table) == {'foo': 'project_id'}


# noinspection PyShadowingNames
def test_apply_mapping_toggles_the_correct_fields(input_row, lookup_table):
    output_row = apply_mapping(deepcopy(input_row), mapping=lookup_table)
    assert set(output_row) ^ set(input_row) == {'project_id', 'foo'}


# noinspection PyShadowingNames
def test_apply_mapping_returns_correct_row(input_row, lookup_table):
    expected_output = {
        'project_id': 1,
        'bar': 2,
        'spam': 3,
        'eggs': 4
    }
    assert apply_mapping(input_row, mapping=lookup_table) == expected_output


# noinspection PyShadowingNames
def test_apply_mapping_with_empty_lookup_returns_identical_row(input_row):
    assert apply_mapping(input_row, mapping=None) == input_row
