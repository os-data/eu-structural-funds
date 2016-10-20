"""Test for the bootstrap module."""

from common.bootstrap import collect_sources
from pytest import mark


@mark.parametrize('source', collect_sources())
def test_source_description_files_are_valid(source):
    assert not source.validation_errors
