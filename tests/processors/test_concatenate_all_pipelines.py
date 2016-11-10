"""Unit-tests for the `concatenate_all_pipelines` processor."""

from datapackage import DataPackage
from os.path import splitext
from tabulator import Stream

from common.processors.concatenate_all_pipelines import (
    assemble_fiscal_datapackage,
    format_data_sample,
    collect_local_datasets,
    concatenate,
    STREAM_OPTIONS)

column_name = 'col1'
dummy_data = '{}\nfoo\nbar'.format(column_name)
more_dummy_data = '{}\nspam\neggs'.format(column_name)


def test_describe_returns_a_dict():
    assert isinstance(assemble_fiscal_datapackage(), dict)


def test_describe_returns_a_valid_fiscal_descriptor():
    datapackage = assemble_fiscal_datapackage()
    assert DataPackage(datapackage, schema='fiscal').validate() is None


def test_format_as_table():
    expected = (
        "+-------+\n"
        "| col1  |\n"
        "+=======+\n"
        "| 'foo' |\n"
        "+-------+\n"
        "| 'bar' |\n"
        "+-------+\n"
    )
    with Stream(dummy_data, **STREAM_OPTIONS) as stream:
        assert format_data_sample(stream) == expected


def test_collect_local_datasets():
    # I shouldn't use the actual data to run tests but hey
    datasets = collect_local_datasets(select={'country_code': 'FR'})
    for filename, csv_text in datasets:
        _, extension = splitext(filename)

        assert isinstance(filename, str)
        assert isinstance(csv_text, str)
        assert extension in ('.csv', '.json', '.xls', 'xlsx')

        # TODO: ensure the dump processor toggles file extensions to csv


def test_concatenate():
    csv_text = [dummy_data, more_dummy_data]
    filenames = ['foobar.csv', 'spameggs.csv']
    resource = list(concatenate(zip(filenames, csv_text)))

    assert len(resource) == 4
    assert all(map(lambda x: isinstance(x, dict), resource))
    assert all(map(lambda x: list(x.keys()).pop() == column_name, resource))
