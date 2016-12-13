"""Unit tests for specifications files."""

import os
import yaml
import json

from pytest import mark
from requests import get

from common.bootstrap import collect_sources
from common.utilities import get_fiscal_datapackage
from common.config import (
    FISCAL_SCHEMA_FILE,
    OS_TYPES_URL,
    FISCAL_MODEL_FILE,
    FISCAL_METADATA_FILE
)


# Constraints
# -----------------------------------------------------------------------------

datapackage_parts = {
    FISCAL_MODEL_FILE,
    FISCAL_SCHEMA_FILE,
    FISCAL_METADATA_FILE
}

required_properties = {
    'name',
    'type',
    'osType',
    'conceptType',
    'format',
    'title',
    'description',
    'slug'
}


def get_valid_os_types():
    response = get(OS_TYPES_URL)
    return json.loads(response.text)


valid_os_types = get_valid_os_types()


# Load datapackage schema and model
# -----------------------------------------------------------------------------

def get_fiscal_fields(section):
    with open(os.path.join(FISCAL_SCHEMA_FILE)) as stream:
        text = stream.read()
    schema = yaml.load(text)
    return schema[section]


def get_model(section):
    with open(os.path.join(FISCAL_MODEL_FILE)) as stream:
        text = stream.read()
    model = yaml.load(text)
    return model[section]


fields = get_fiscal_fields('fields')
measures = get_model('measures')
dimensions = get_model('dimensions')


# Test datapackage
# -----------------------------------------------------------------------------

@mark.parametrize('file', datapackage_parts)
def test_datapackage_parts_are_dicts(file):
    with open(file) as stream:
        contents = yaml.load(stream.read())
        assert isinstance(contents, dict)


def test_fiscal_datapackage_is_valid():
    assert get_fiscal_datapackage(skip_validation=False)


# Test schema
# -----------------------------------------------------------------------------

@mark.parametrize('field', fields)
def test_each_fiscal_schema_field_is_a_dict(field):
    assert isinstance(field, dict)


@mark.parametrize('field', fields)
def test_fiscal_schema_fields_properties_are_not_empty(field):
    assert all([k for k, v in field.items() if k != 'groupChar'])


@mark.parametrize('field', fields)
def test_fiscal_schema_fields_have_required_properties(field):
    if field['osType'] != 'value':
        assert set(field.keys()) == required_properties


@mark.parametrize('field', fields)
def test_fiscal_value_fields_have_extra_properties(field):
    if field['osType'] == 'value':
        assert 'decimalChar' in set(field.keys())


@mark.parametrize('field', fields)
def test_all_fiscal_schema_fields_properties_are_strings(field):
    assert all(map(lambda x: isinstance(x, str), field.keys()))


@mark.parametrize('field', fields)
def test_fiscal_schema_fields_properties_have_correct_values(field):
    assert field['conceptType'] == field['osType'].split(':')[0]
    assert field['type'] in ('number', 'string', 'date')
    assert field['format'] == 'default'
    assert field['slug'] == field['name']
    assert field['osType'] in valid_os_types


@mark.parametrize('field', fields)
def test_fiscal_schema_slugs_are_pure_ascii(field):
    assert len(field['slug']) == len(field['slug'].encode())


# Test model
# -----------------------------------------------------------------------------

def test_fiscal_model_measures_match_schema_value_types():
    value_keys = [key['slug'] for key in fields if key['osType'] == 'value']
    assert set(measures.keys()) == set(value_keys)


def test_model_dimensions_are_valid_os_top_level_types():
    top_level_types = map(lambda x: x.split(':')[0], valid_os_types.keys())
    assert set(dimensions.keys()) <= set(top_level_types)


@mark.parametrize('field', fields)
def test_fiscal_model_measures_name_and_title_match_schema(field):
    if field['osType'] == 'value':
        assert measures[field['slug']]['title'] == field['title']
        assert measures[field['slug']]['source'] == field['name']


@mark.parametrize('dimension', dimensions.values())
def test_fiscal_model_dimensions_source_and_title_match_schema(dimension):
    for properties in dimension['attributes'].values():
        assert properties['source'] in [field['slug'] for field in fields]
        assert properties['title'] in [field['title'] for field in fields]


@mark.parametrize('dimension', dimensions.values())
def test_fiscal_model_dimension_keys_equals_source_property(dimension):
    for attribute, properties in dimension['attributes'].items():
        assert properties['source'] == attribute


@mark.parametrize('dimension', dimensions.values())
def test_fiscal_model_dimension_keys_are_a_subset_of_attributes(dimension):
    assert set(dimension['primaryKey']) <= set(dimension['attributes'].keys())


@mark.parametrize('dimension', dimensions.values())
def test_fiscal_model_primary_keys_are_a_subset_of_attributes(dimension):
    assert set(dimension['primaryKey']) <= set(dimension['attributes'].keys())


@mark.parametrize('dimension', dimensions.values())
def test_labelfor_and_parent_properties_point_to_an_attribute(dimension):
    for attribute in dimension['attributes'].values():
        for pointer in ('labelfor', 'parent'):
            if pointer in attribute.keys():
                assert attribute[pointer] in dimension['attributes']


@mark.parametrize('dimension', dimensions.values())
def test_model_parent_attributes_belong_to_primary_keys(dimension):
    for attribute in dimension['attributes']:
        if 'parent' in attribute:
            assert attribute['parent'] in dimension['primaryKey']


# Validate all data source descriptions
# -----------------------------------------------------------------------------

@mark.parametrize('source', collect_sources())
def test_source_description_files_are_valid(source):
    assert not source.validation_errors
