"""Test module for the cast_to_fiscal_types processor."""

from datetime import date
from common.processors.cast_to_fiscal_schema import cast_values, \
    get_fiscal_types
from pytest import mark

fiscal_types = dict(get_fiscal_types())

valid_dates = [
    '2000-12-31',
    '2000/12/31',
]
invalid_dates = [
    '12/31/2000',
    '12-31-2000',
    '31-12-2000',
]
numbers = [
    '1',
    '1.',
    '1.0',
    1,
    1.0,
    1e0
]
strings = [
    (1.0, '1.0'),
    (1., '1.0'),
    (1, '1'),
    ('1', '1')
]
type_examples = [
    'approval_date',
    'total_amount',
    'fund_name'
]


@mark.parametrize('value', valid_dates)
def test_cast_values_casts_dates_correctly(value):
    expected = {'approval_date': date(2000, 12, 31)}
    assert cast_values({'approval_date': value}) == expected


@mark.parametrize('value', invalid_dates)
def test_cast_values_returns_bad_guess(value):
    expected = {'approval_date': date(2000, 12, 31)}
    assert cast_values({'approval_date': value}) != expected


def test_cast_values_handles_date_objects_correctly():
    expected = {'approval_date': date(2000, 12, 31)}
    date_object = {'approval_date': date(2000, 12, 31)}
    assert cast_values(date_object) == expected


@mark.parametrize('value', numbers)
def test_cast_values_casts_to_numbers_correctly(value):
    expected = {'total_amount': 1.0}
    assert cast_values({'total_amount': value}) == expected


@mark.parametrize('value, expected', strings)
def test_cast_values_casts_to_strings_correctly(value, expected):
    expected_row = {'fund_name': expected}
    assert cast_values({'fund_name': value}) == expected_row


@mark.parametrize('field_name', type_examples)
def test_cast_values_handles_empty_cells_correctly(field_name):
    empty_cell = {field_name: None}
    assert cast_values(empty_cell) == empty_cell
