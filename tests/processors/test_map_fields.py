"""Unit-tests for the `map_fields` processor."""

import json

from datetime import date
from jsontableschema import Field, Schema
from pytest import fixture, raises

# noinspection PyProtectedMember
from common.processors.map_fields import (
    _build_mapping_tables,
    _update_datapackage,
    _process_row,
    _get_mapping_targets,
    map_fields)

# Dummy data and datapackage
# -----------------------------------------------------------------------------

_MAPPER = 'maps_to'
_IGNORE = ['_ignored', None]
_PARAMS = _MAPPER, _IGNORE


@fixture(scope='function')
def _datapackage():
    fields = [
        {
            'name': 'missing',
            'type': 'integer'
        },
        {
            'name': 'ignored',
            'maps_to': '_ignored',
            'type': 'integer'
        },
        {
            'name': 'none',
            'maps_to': None,
            'type': 'integer'
        },
        {
            'name': 'simple',
            'maps_to': 'foo',
            'type': 'integer'
        },
        {
            'name': 'merge1',
            'maps_to': 'bar',
            'type': 'string'
        },
        {
            'name': 'merge2',
            'maps_to': 'bar',
            'type': 'date'
        },
        {
            'name': 'merge3',
            'maps_to': ['baz', 'spam'],
            'type': 'boolean'
        },
        {
            'name': 'merge4',
            'maps_to': ['baz', 'spam'],
            'type': 'boolean'
        },
        {
            'name': 'split',
            'maps_to': ['bacon', 'eggs'],
            'type': 'number'
        }
    ]
    return {'resources': [{'schema': {'fields': fields}}]}


@fixture(scope='function')
def _resources():
    return [[
        {
            'missing': 0,
            'ignored': 0,
            'none': 0,
            'simple': 1,
            'merge1': 'string',
            'merge2': date(2000, 1, 1),
            'merge3': True,
            'merge4': None,
            'split': 0.1
        }
    ]]


_EXPECTED_MAPPING_TARGETS = [
    [None],
    [None],
    [None],
    ['foo'],
    ['bar'],
    ['bar'],
    ['baz', 'spam'],
    ['baz', 'spam'],
    ['bacon', 'eggs'],
]

_EXPECTED_DATAPACKAGE = {
    'resources': [
        {'schema': {
            'fields': [
                # Fields are listed in ascending order by name for comparison
                {
                    'name': 'bacon',
                    'mapped_from': ['split'],
                    'type': 'number',
                },
                {
                    'name': 'bar',
                    'mapped_from': ['merge1', 'merge2'],
                    'type': 'array'
                },
                {
                    'name': 'baz',
                    'mapped_from': ['merge3', 'merge4'],
                    'type': 'array'
                },
                {
                    'name': 'eggs',
                    'mapped_from': ['split'],
                    'type': 'number'
                },
                {
                    'name': 'foo',
                    'mapped_from': ['simple'],
                    'type': 'integer'
                },
                {
                    'name': 'spam',
                    'mapped_from': ['merge3', 'merge4'],
                    'type': 'array',
                },
            ]
        }
        }
    ]
}

_EXPECTED_TABLE = [[
    ('missing', None),
    ('ignored', None),
    ('none', None),
    ('simple', 'foo'),
    ('merge1', 'bar'),
    ('merge2', 'bar'),
    ('merge3', 'baz'),
    ('merge3', 'spam'),
    ('merge4', 'baz'),
    ('merge4', 'spam'),
    ('split', 'bacon'),
    ('split', 'eggs')
]]

_EXPECTED_ROW = {
    'foo': 1,
    'bar': ['string', date(2000, 1, 1)],
    'spam': [True, None],
    'baz': [True, None],
    'bacon': 0.1,
    'eggs': 0.1

}


# Test the public function
# ------------------------

def test__map_fields__raises_assertion_errors_on_bad_arguments():
    with raises(AssertionError):
        # noinspection PyTypeChecker
        map_fields({}, [[]], mapper=999)

    with raises(AssertionError):
        # noinspection PyTypeChecker
        map_fields({}, [[]], mapper=None)

    with raises(AssertionError):
        # noinspection PyTypeChecker
        map_fields({}, [[]], ignore='string')


# noinspection PyShadowingNames
def test__map_fields__outputs_correct_object_types(_datapackage, _resources):
    updated_datapackage, new_resources, tables = \
        map_fields(_datapackage, _resources)

    assert isinstance(updated_datapackage, dict)
    assert isinstance(tables[0][0], tuple)
    assert isinstance(next(next(new_resources)), dict)


# noinspection PyShadowingNames
def test__map_fields__writes_to_log(_datapackage, caplog):
    map_fields(_datapackage, _resources)
    assert caplog.records()[0].levelname == 'INFO'
    assert caplog.records()[1].levelname == 'INFO'


# Test building the mapping table
# -----------------------------------------------------------------------------

# noinspection PyShadowingNames
def test_get_mapping_targets__returns_correct_names(_datapackage):
    fields = _datapackage['resources'][0]['schema']['fields']
    mapping_tests = zip(fields, _EXPECTED_MAPPING_TARGETS)
    for field, expected_mapping_target in mapping_tests:
        mapping_target = _get_mapping_targets(Field(field), _MAPPER, _IGNORE)
        assert list(mapping_target) == expected_mapping_target


# noinspection PyShadowingNames
def test__build_mapping_tables__returns_a_list_of_lists_of_tuples(
        _datapackage):
    tables = _build_mapping_tables(_datapackage, *_PARAMS)
    assert isinstance(tables, list)

    for table in tables:
        assert isinstance(table, list)
        for mapping in table:
            assert isinstance(mapping, tuple)


# noinspection PyShadowingNames
def test__build_mapping_tables__returns_expected_number_of_tables(
        _datapackage):
    tables = _build_mapping_tables(_datapackage, *_PARAMS)
    assert len(tables) == len(_datapackage['resources'])


# noinspection PyShadowingNames
def test__build_mapping_tables__returns_tables_of_correct_length(_datapackage):
    tables = _build_mapping_tables(_datapackage, *_PARAMS)
    assert len(tables[0]) == len(_EXPECTED_TABLE[0])


# noinspection PyShadowingNames
def test__build_mapping_tables__returns_expected_mappings(_datapackage):
    assert _build_mapping_tables(_datapackage, *_PARAMS) == _EXPECTED_TABLE


# noinspection PyShadowingNames
def test__build_mapping_table__raises_error_on_bad_mapping(_datapackage):
    field = {'maps_to': 42, 'name': 'bad'}
    _datapackage['resources'][0]['schema']['fields'] = [field]
    with raises(ValueError):
        _build_mapping_tables(_datapackage, *_PARAMS)


def test__build_mapping_tables__raises_type_error_on_unsupported_jts_type():
    unsupported_fields = [{'name': 'foo', 'type': 'array', 'maps_to': 'bar'}]
    datapackage = {'resources': [{'schema': {'fields': unsupported_fields}}]}
    with raises(TypeError):
        _build_mapping_tables(datapackage, *_PARAMS)


# Test the datapackage update
# -----------------------------------------------------------------------------

# noinspection PyShadowingNames
def test__update_datapackage__returns_expected_set_of_fields(_datapackage):
    tables = _build_mapping_tables(_datapackage, *_PARAMS)
    updated_datapackage = _update_datapackage(_datapackage, tables, _IGNORE)
    schema = Schema(updated_datapackage['resources'][0]['schema'])
    expected_schema = Schema(_EXPECTED_DATAPACKAGE['resources'][0]['schema'])

    assert set([field.name for field in schema.fields]) == \
           set([field.name for field in expected_schema.fields])


# noinspection PyShadowingNames
def test__update_datapackage__returns_expected_modifications(_datapackage):
    tables = _build_mapping_tables(_datapackage, *_PARAMS)
    updated_datapackage = _update_datapackage(_datapackage, tables, _IGNORE)
    fields = updated_datapackage['resources'][0]['schema']['fields']
    fields.sort(key=lambda x: x['name'])

    assert json.dumps(updated_datapackage, sort_keys=True) == \
           json.dumps(_EXPECTED_DATAPACKAGE, sort_keys=True)


# noinspection PyShadowingNames
def test__update_datapackage__returns_required_field_properties(_datapackage):
    tables = _build_mapping_tables(_datapackage, *_PARAMS)
    updated_datapackage = _update_datapackage(_datapackage, tables, _IGNORE)
    required_properties = {'mapped_from', 'name', 'type'}

    for field in updated_datapackage['resources'][0]['schema']['fields']:
        assert required_properties == set(field)
        assert 'maps_to' not in field


# Test the data processing
# -----------------------------------------------------------------------------

# noinspection PyShadowingNames
def test__process_row__returns_expected_row(_datapackage, _resources):
    tables = _build_mapping_tables(_datapackage, *_PARAMS)
    new_row = _process_row(_resources[0][0], tables, _IGNORE, 0)
    assert new_row == _EXPECTED_ROW


# noinspection PyShadowingNames
def test__apply_mapping__passes_data_validation(_datapackage, _resources):
    updated_datapackage, new_resources, _ = \
        map_fields(_datapackage, _resources, mapper=_MAPPER, ignore=_IGNORE)
    schema = Schema(updated_datapackage['resources'][0]['schema'])
    new_row = next(next(new_resources))

    for name, value in new_row.items():
        assert schema.get_field(name).cast_value(value) == value
