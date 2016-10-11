"""Test the common reshape_data processor."""

from pytest import mark
from common.processors.reshape_data import process_resources, process_row

test_cases = [
    ({1: 'a', 2: 'b'}, [1, 2, 3], {1: 'a', 2: 'b', 3: None}),
    ({1: 'a', 4: 'd'}, [1, 2, 3], {1: 'a', 2: None, 3: None})
]


@mark.parametrize('input_row, required_fields, expected_output', test_cases)
def test_process_rows_function(input_row, required_fields, expected_output):
    assert process_row(input_row, required_fields) == expected_output


def test_process_resources_function():
    output_resources = process_resources([[{1: 'a', 2: 'b'}]], [1, 3])
    assert next(next(output_resources)) == {1: 'a', 3: None}
