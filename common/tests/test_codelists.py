"""Validation tests for codelists."""

from pytest import mark

from common.utilities import get_codelist, get_all_codelists

all_codelists = get_all_codelists().values()


def test_funding_period_codelist():
    codelist = get_codelist('funding_period_number')
    expected_mapping = {1: '2000-2006', 2: '2007-2013', 3: '2014-2020'}
    assert codelist['mapping'] == expected_mapping
    assert codelist['label'] == 'funding_period'


@mark.parametrize('codelist', all_codelists)
def test_codelists_specify_label_and_mapping(codelist):
    assert 'label' in codelist
    assert 'mapping' in codelist


@mark.parametrize('codelist', all_codelists)
def test_codelist_mappings_are_dicts(codelist):
    mapping = codelist['mapping']
    assert isinstance(mapping, dict)
