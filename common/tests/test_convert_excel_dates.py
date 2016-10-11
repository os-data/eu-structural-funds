"""Unit-tests for the convert_excel_dates processor."""

from common.processors.convert_excel_dates import convert_dates, \
    process_resources
from datetime import date


def test_convert_dates_returns_correct_row():
    expected = {'date1': date(2015, 6, 5)}
    assert convert_dates({'date1': 42160}, ['date1'], 0) == expected


def test_process_resources_returns_correct_resources():
    expected_row = {'date1': date(1954, 10, 3)}
    input_resource = [{'date1': 20000}]
    output_resources = process_resources([input_resource], ['date1'], 0)
    assert next(next(output_resources)) == expected_row
