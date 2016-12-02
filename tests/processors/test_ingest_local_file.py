"""Unit-tests for the `ingest_local_file` processor."""

from common.processors.ingest_local_file import BaseIngestor


# noinspection PyProtectedMember
def test_lowercase_empty_values():
    assert next(BaseIngestor._lowercase_empty_values(
        [(1, ['foo'], ['bar'])])) == (1, ['foo'], ['bar'])
    assert next(BaseIngestor._lowercase_empty_values(
        [(1, ['foo'], ['None'])])) == (1, ['foo'], ['none'])
