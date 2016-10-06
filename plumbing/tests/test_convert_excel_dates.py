"""Unit-tests for the convert_excel_dates processor."""

from common.convert_excel_dates import process_row, process_resources
from datetime import datetime


def test_process_row_converts_to_correct_dates():
    expected = {'date1': datetime(2015, 6, 5, 0, 0)}
    assert process_row({'date1': 42160}, ['date1'], 0) == expected


def test_process_rows_converts_to_correct_dates():
    expected = {'date1': datetime(1954, 10, 3, 0, 0)}
    input_rows = [{'date1': 20000}]
    new_resources = process_resources([input_rows], ['date1'], 0)
    assert next(next(new_resources)) == expected
