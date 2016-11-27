"""Unit-tests for the `concatenate_all_pipelines` processor."""

from datapackage import DataPackage
from os.path import splitext
from pytest import fixture
from pytest import raises
from tabulator import Stream

from common.processors.concatenate_all_pipelines import (
    assemble_fiscal_datapackage,
    format_data_sample,
    collect_local_datasets,
    concatenate,
    STREAM_OPTIONS)


def test_assemble_fiscal_datapackage_returns_a_dict():
    assert isinstance(assemble_fiscal_datapackage(), dict)


def test_assemble_fiscal_datapackage_returns_a_valid_fiscal_descriptor():
    datapackage = assemble_fiscal_datapackage()
    # The validation raises an exception if the validation fails
    assert DataPackage(datapackage, schema='fiscal').validate() is None


def test_format_as_table():
    dummy_data = 'col1\nfoo\nbar'
    expected_representation = (
        "+-------+\n"
        "| col1  |\n"
        "+=======+\n"
        "| 'foo' |\n"
        "+-------+\n"
        "| 'bar' |\n"
        "+-------+\n"
    )
    with Stream(dummy_data, **STREAM_OPTIONS) as stream:
        assert format_data_sample(stream) == expected_representation


def test_collect_local_datasets():
    # I shouldn't use the actual data to run tests but hey
    datasets_ = collect_local_datasets(pipelines={'country_code': 'FR'})
    for filename, csv_text in datasets_:
        _, extension = splitext(filename)

        assert isinstance(filename, str)
        assert isinstance(csv_text, str)
        assert extension in ('.csv', '.json', '.xls', 'xlsx')

        # TODO: ensure the dump processor toggles file extensions to csv


@fixture
def datasets():
    csv_texts = [
        'beneficiary_name, starting_date\nfoo,eggs',
        'beneficiary_name, starting_date\nbar,spam'
    ]
    filenames = ['foobar.csv', 'spameggs.csv']
    return list(zip(filenames, csv_texts))


# noinspection PyShadowingNames
def test_concatenate_all_fields(datasets):
    resource = list(concatenate(datasets))
    expected_fields = {'beneficiary_name', 'starting_date'}

    assert len(resource) == 2
    assert isinstance(resource, list)
    assert all(map(lambda x: isinstance(x, dict), resource))
    assert all(map(lambda x: set(x.keys()) == expected_fields, resource))


# noinspection PyShadowingNames
def test_concatenate_a_subset_of_fields(datasets):
    resource = list(concatenate(datasets, fields=['beneficiary_name']))

    assert all(map(lambda x: set(x.keys()) == {'beneficiary_name'}, resource))
    assert len(resource) == 2


# noinspection PyShadowingNames
def test_concatenate_raises_assertion_error_on_invalid_field(datasets):
    with raises(ValueError):
        list(concatenate(datasets, fields=['invalid_field']))
