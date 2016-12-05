"""Unit-tests for the `map_fields` processor."""

from copy import deepcopy
from pytest import mark
from pytest import raises

from common.utilities import FISCAL_KEYS
from common.processors.map_fields import (
    build_mapping_tables,
    update_datapackage,
    apply_mapping,
    check_mapping_target)

_DATAPACKAGE = {
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

_VALID_MAPPINGS = [
    FISCAL_KEYS[0],
    [FISCAL_KEYS[1], FISCAL_KEYS[2]],
]


@mark.parametrize('valid_mapping', _VALID_MAPPINGS)
def test_check_mapping_asserts_error_on_invalid_mapping(valid_mapping):
    assert isinstance(check_mapping_target(valid_mapping), type(valid_mapping))


_INVALID_MAPPINGS = [
    'bad',
    ['very_bad', 'worse'],
    [FISCAL_KEYS[0], 'extra_worse']
]


@mark.parametrize('invalid_mapping', _INVALID_MAPPINGS)
def test_check_mapping_asserts_error_on_invalid_mapping(invalid_mapping):
    with raises(AssertionError):
        check_mapping_target(invalid_mapping)


# Test the mapping lookup table
# -----------------------------------------------------------------------------

def test_build_mapping_tables_returns_a_list_of_dicts():
    mappings = build_mapping_tables(_DATAPACKAGE)
    assert isinstance(mappings, list)
    for mapping in mappings:
        assert isinstance(mapping, dict)


def test_build_mapping_tables_output_has_correct_size():
    mappings = build_mapping_tables(_DATAPACKAGE)
    assert len(mappings) == len(_DATAPACKAGE['resources'])
    for mapping, resource in zip(mappings, _DATAPACKAGE['resources']):
        assert len(mapping) == len(resource['schema']['fields'])


def test_build_mapping_tables_returns_correct_mappings():
    assert build_mapping_tables(_DATAPACKAGE) == [
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

def test_update_datapackage_deletes_maps_to_property():
    mappings = build_mapping_tables(_DATAPACKAGE)
    mutated_datapackage = update_datapackage(deepcopy(_DATAPACKAGE), mappings)
    for resource in mutated_datapackage['resources']:
        assert 'maps_to' not in resource['schema']['fields']


def test_update_datapackage_returns_correct_mutated_dict():
    mappings = build_mapping_tables(_DATAPACKAGE)
    mutated_datapackage = update_datapackage(deepcopy(_DATAPACKAGE), mappings)
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


def test_apply_mapping_returns_correct_row():
    mappings = build_mapping_tables(_DATAPACKAGE)
    assert apply_mapping(deepcopy(_DUMMY_ROW),
                         mappings=mappings,
                         resource_index=0) == {
               FISCAL_KEYS[0]: '1',
               FISCAL_KEYS[1]: '5',
               FISCAL_KEYS[2]: '5'
           }
