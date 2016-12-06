"""Unit-tests for the `map_fields` processor."""

from copy import deepcopy

from pytest import fixture
from pytest import mark
from pytest import raises

from common.utilities import FISCAL_KEYS
from common.processors.map_fields import (
    build_mapping_tables,
    update_datapackage,
    apply_mapping,
    check_mapping_target)


@fixture(scope='function')
def _datapackage():
    return {
        'resources': [
            {
                'schema':
                    {
                        'fields': [
                            {'name': 'bacon'},
                            {'name': 'foo', 'maps_to': FISCAL_KEYS[0]},
                            {'name': 'bar', 'maps_to': '_ignored'},
                            {'name': 'baz', 'maps_to': None},
                            {'name': 'spam', 'maps_to': ''},
                            {'name': 'eggs', 'maps_to': [FISCAL_KEYS[1],
                                                         FISCAL_KEYS[2]]}
                        ]
                    }
            }
        ]
    }


# Test the mapping lookup table
# -----------------------------------------------------------------------------

_valid_mappings = [
    FISCAL_KEYS[0],
    [FISCAL_KEYS[1], FISCAL_KEYS[2]],
]


@mark.parametrize('valid_mapping', _valid_mappings)
def test_check_mapping_asserts_error_on_invalid_mapping(valid_mapping):
    assert isinstance(check_mapping_target(valid_mapping), type(valid_mapping))


_invalid_mappings = [
    'bad',
    ['very_bad', 'worse'],
    [FISCAL_KEYS[0], 'extra_worse']
]


@mark.parametrize('invalid_mapping', _invalid_mappings)
def test_check_mapping_asserts_error_on_invalid_mapping(invalid_mapping):
    with raises(AssertionError):
        check_mapping_target(invalid_mapping)


# Test the mapping lookup table
# -----------------------------------------------------------------------------

# noinspection PyShadowingNames
def test_build_mapping_tables_returns_a_list_of_dicts(_datapackage):
    mappings = build_mapping_tables(_datapackage)
    assert isinstance(mappings, list)
    for mapping in mappings:
        assert isinstance(mapping, dict)


# noinspection PyShadowingNames
def test_build_mapping_tables_output_has_correct_size(_datapackage):
    mappings = build_mapping_tables(_datapackage)
    assert len(mappings) == len(_datapackage['resources'])
    for mapping, resource in zip(mappings, _datapackage['resources']):
        assert len(mapping) == len(resource['schema']['fields'])


# noinspection PyShadowingNames
def test_build_mapping_tables_returns_correct_mappings(_datapackage):
    assert build_mapping_tables(_datapackage) == [
        {
            'bacon': None,
            'foo': FISCAL_KEYS[0],
            'bar': '_ignored',
            'baz': None,
            'spam': '',
            'eggs': [FISCAL_KEYS[1], FISCAL_KEYS[2]]
        },
    ]


# Test datapackage mutation
# -----------------------------------------------------------------------------

# noinspection PyShadowingNames
def test_update_datapackage_deletes_maps_to_property(_datapackage):
    mappings = build_mapping_tables(_datapackage)
    mutated_datapackage = update_datapackage(deepcopy(_datapackage), mappings)
    for resource in mutated_datapackage['resources']:
        assert 'maps_to' not in resource['schema']['fields']


# noinspection PyShadowingNames
def test_update_datapackage_returns_correct_mutated_dict(_datapackage):
    mappings = build_mapping_tables(_datapackage)
    mutated_datapackage = update_datapackage(deepcopy(_datapackage), mappings)
    new_fields = mutated_datapackage['resources'][0]['schema']['fields']
    assert {'name': FISCAL_KEYS[0], 'mapped_from': 'foo'} in new_fields
    assert {'name': FISCAL_KEYS[1], 'mapped_from': 'eggs'} in new_fields
    assert {'name': FISCAL_KEYS[2], 'mapped_from': 'eggs'} in new_fields
    assert len(new_fields) == 3


# Test datapackage mutation
# -----------------------------------------------------------------------------

_DUMMY_ROW = {
    'bacon': 0,
    'foo': '1',
    'bar': '2',
    'baz': '3',
    'spam': '4',
    'eggs': '5'
}


# noinspection PyShadowingNames
def test_apply_mapping_returns_correct_row(_datapackage):
    mappings = build_mapping_tables(_datapackage)
    assert apply_mapping(deepcopy(_DUMMY_ROW),
                         mappings=mappings,
                         resource_index=0) == {
               FISCAL_KEYS[0]: '1',
               FISCAL_KEYS[1]: '5',
               FISCAL_KEYS[2]: '5'
           }
