"""Test module for the Malta parse_start_date processor."""

from pytest import mark
import datetime
from common.processors.MT.parse_dates import parse_dates

date_examples = [
    ('2011', (2011, 1, 1)),
    ('Q2 2011', (2011, 4, 1)),
    ('Q 4 2011', (2011, 10, 1)),
    ('Quarter 3 2011', (2011, 7, 1)),
    ('Quarter2 2011', (2011, 4, 1)),
    ('Tuesday, October 18, 2011', (2011, 10, 18)),
    ('31/01/2011', (2011, 1, 31))
]


@mark.parametrize('raw_date, date_tuple', date_examples)
def test_parse_start_date_parses_correctly(raw_date, date_tuple):
    row = {'Start Date': raw_date}
    result = parse_dates(row, date_fields=['Start Date'])['Start Date']
    assert result == datetime.date(*date_tuple)
