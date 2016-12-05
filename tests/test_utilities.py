"""Unit tests for the utilities module."""

import os

from glob import glob
from unittest import skipIf
from unittest.mock import mock_open, patch

from common.config import CODELISTS_DIR, DATA_DIR
from common.utilities import (
    get_codelist,
    get_fiscal_datapackage,
    process,
    get_nuts_codes,
    get_available_processors,
    GEOCODES)


def test_get_available_processors_returns_list_of_strings():
    modules = get_available_processors()
    assert len(modules) > 0
    assert isinstance(modules, list)
    for processor in modules:
        assert isinstance(processor, str)


@skipIf(True, 'Travis cannot find the build-in open object')
@patch(
    get_codelist.__module__ + '.open',
    new_callable=mock_open
)
def test_get_codelists_opens_correct_file(mocked):
    get_codelist('foo')
    codelist_file = os.path.join(CODELISTS_DIR, 'foo.yaml')
    mocked.assert_called_once_with(codelist_file)


@skipIf(True, 'Travis cannot find the build-in open object')
@patch(
    get_fiscal_datapackage.__module__ + '.open',
    new_callable=mock_open,
    read_data='{"resources": [{"schema": None}]}'
)
def test_get_fiscal_datapackage_assembles_datapackage_from_parts(mocked):
    datapackage = get_fiscal_datapackage(skip_validation=True)
    assert mocked.call_count == 3
    assert isinstance(datapackage, dict)
    assert 'model' in datapackage


def test_process_returns_a_generator_of_generators():
    assert next(next(process([['foo']], lambda x: x))) == 'foo'


def test_nuts_codes_in_data_tree_are_valid():
    nuts_codes = get_nuts_codes()
    for node in glob(DATA_DIR + '/*/*'):
        if os.path.isdir(os.path.join(os.path.dirname(__file__), node)):
            parts = node.split('/')[-1].split('.')
            if len(parts) == 2:
                like_geofolder = (
                    parts[0].isupper(),
                    parts[1].islower()
                )
                if all(like_geofolder):
                    assert parts[0] in nuts_codes


def test_get_geocodes_returns_a_list_of_dicts():
    assert isinstance(GEOCODES, list)
    for line in GEOCODES:
        assert isinstance(line, dict)
