"""Test module for the `fill_in_constant_fields` processor."""

from common.processors.fill_in_constant_fields import fill_columns


def test_fill_columns_without_constants_returns_identical_row():
    assert fill_columns({'foo': 'bar'}) == {'foo': 'bar'}


def test_fill_columns_returns_correct_row():
    new_row = fill_columns({'foo': None}, constants={'foo': 'bar'})
    assert new_row == {'foo': 'bar'}
