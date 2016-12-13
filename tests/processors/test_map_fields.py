"""Unit-tests for the `map_fields` processor."""

from copy import deepcopy

from pytest import raises
from common.processors.map_fields import (
    build_mapping_tables,
    update_datapackage,
    apply_mapping
)

datapackage = {
    'resources':
        [
            {
                'schema':
                    {
                        'fields':
                            [
                                {
                                    'name': 'foo',
                                    'maps_to': 'beneficiary_name'
                                },
                                {
                                    'name': 'bar',
                                    'maps_to': '_ignored'
                                }
                            ]
                    }
            },
            {
                'schema':
                    {
                        'fields':
                            [
                                {
                                    'name': 'spam',
                                    'maps_to': None
                                },
                                {
                                    'name': 'eggs',
                                    'maps_to': '_unknown'
                                }
                            ]
                    }
            }
        ]
}

expected_mappings = [
    {
        'foo': 'beneficiary_name',
        'bar': '_ignored'

    },
    {
        'spam': '_unknown',
        'eggs': '_unknown'
    },
]

mutated_datapackage = {
    'resources':
        [
            {
                'schema':
                    {
                        'fields':
                            [
                                {
                                    'name': 'beneficiary_name'
                                },
                                {
                                    'name': '_ignored'
                                }
                            ]
                    }
            },
            {
                'schema':
                    {
                        'fields':
                            [
                                {
                                    'name': '_unknown'
                                },
                                {
                                    'name': '_unknown',
                                }
                            ]
                    }
            }
        ]
}

bad_datapackage = {
    'resources': [
        {
            'schema':
                {
                    'fields':
                        [
                            {
                                'name': 'foo',
                                'maps_to': 'invalid_mapping'

                            }
                        ]
                }
        }
    ]
}

row = {
    'foo': 1,
    'bar': 2,
    'spam': 3,
    'eggs': 4,
}


def test_build_mapping_tables_returns_a_list_of_dicts():
    mappings = build_mapping_tables(datapackage)
    assert isinstance(mappings, list)
    for mapping in mappings:
        assert isinstance(mapping, dict)


def test_build_mapping_tables_output_has_correct_size():
    mappings = build_mapping_tables(datapackage)
    assert len(mappings) == len(datapackage['resources'])
    for mapping, resource in zip(mappings, datapackage['resources']):
        assert len(mapping) == len(resource['schema']['fields'])


def test_build_mapping_tables_raises_assert_error_on_invalid_mapping_value():
    with raises(AssertionError):
        build_mapping_tables(bad_datapackage)


def test_build_mapping_tables_returns_correct_mapping():
    assert build_mapping_tables(datapackage) == expected_mappings


def test_update_datapackage_deletes_maps_to_property():
    mappings = build_mapping_tables(datapackage)
    new_datapackage = update_datapackage(deepcopy(datapackage), mappings)
    for resource in new_datapackage['resources']:
        assert 'maps_to' not in resource['schema']['fields']


def test_update_datapackage_renames_fields_correctly():
    mappings = build_mapping_tables(datapackage)
    update_datapackage(deepcopy(datapackage), mappings) == mutated_datapackage


def test_apply_mapping_returns_correct_row():
    mappings = build_mapping_tables(datapackage)
    assert apply_mapping(
        deepcopy(row),
        mappings=mappings,
        resource_index=0
    ) == {'beneficiary_name': 1, 'spam': 3, 'eggs': 4}
    assert apply_mapping(
        deepcopy(row),
        mappings=mappings,
        resource_index=1
    ) == {'foo': 1, 'bar': 2}
