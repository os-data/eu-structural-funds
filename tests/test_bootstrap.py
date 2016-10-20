"""Test for the bootstrap module."""

import sys
print(sys.path)
from common.bootstrap import collect_sources
from pytest import mark


@mark.parametrize('source', collect_sources())
def test_source_description_files_are_valid(source):
    assert not source.validation_errors
