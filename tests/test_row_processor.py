"""Unit-tests for the `row_processor` module."""

import logging
import unittest
import pytest

# noinspection PyProtectedMember
from common.row_processor import (
    _write_to_log,
    _override_parameters,
    _get_sample_rows,
    process,
    _check_parameters)


# Test helper functions
# -----------------------------------------------------------------------------

def test__write_to_log__writes_to_the_log_at_info_level(caplog):
    _write_to_log({'parameter_key': 'parameter_value'}, [{'foo': 'bar'}], 0)
    caplog.records()[0].level = logging.INFO
    caplog.records()[1].level = logging.INFO


def test__override_parameters__overrides_parameters_if_possible():
    default_parameters = {'parameter': 'pipeline'}

    datapackage = {'name': 'foo', 'resources': [{}]}
    assert _override_parameters(
        default_parameters, datapackage, 0,
        processor_name='processor')['parameter'] == 'pipeline'

    datapackage_parameters = {'processor': {'parameter': 'datapackage'}}
    datapackage.update(datapackage_parameters)
    assert _override_parameters(
        default_parameters, datapackage, 0,
        processor_name='processor')['parameter'] == 'datapackage'

    resource_parameter = {'processor': {'parameter': 'resource 0'}}
    datapackage['resources'][0].update(resource_parameter)
    assert _override_parameters(
        default_parameters, datapackage, 0,
        processor_name='processor')['parameter'] == 'resource 0'


def test__check_processing_parameters__raises_error_if_not_dict():
    with pytest.raises(TypeError):
        _check_parameters(['bad parameter type'])


def test__collect_sample_rows__returns_expected_sample():
    rows = [{'foo': 'bar'} for _ in range(0, 100)]
    expected_sample = list(_get_sample_rows(rows, 10))
    assert all(list(map(lambda x: x == {'foo': 'bar'}, expected_sample)))
    assert len(expected_sample) == 10


# Test passing context and parameters to the row processor
# -----------------------------------------------------------------------------


class TestContext(unittest.TestCase):
    pass_context = True
    parameters = {}

    expected_stats = [{}]
    expected_resource = [
        {'foo': 0},
        {'foo': 11},
        {'foo': 2},
        {'foo': 13}
    ]

    def setUp(self):
        self.resources = [[
            {'foo': 0},
            {'foo': 1},
            {'foo': 2},
            {'foo': 3}
        ]]

    def do_test(self):
        new_resources, stats = process(
            self.resources,
            self.row_processor,
            pass_context=self.pass_context,
            parameters=self.parameters
        )
        self.assertListEqual(list(next(new_resources)), self.expected_resource)
        self.assertListEqual(stats, self.expected_stats)

    @staticmethod
    def row_processor(row, index):
        if index.row % 2:
            row['foo'] += 10
        return row, '_pass'

    def test__process__handles_context(self):
        self.do_test()


class TestParameters(TestContext):
    pass_context = False
    parameters = {'predicate': lambda x: x % 2}

    @staticmethod
    def row_processor(row, predicate=None):
        if predicate(row['foo']):
            row['foo'] += 10
        return row, '_pass'

    def test__process__handles_parameters(self):
        self.do_test()


# Test test_collecting statistics
# -----------------------------------------------------------------------------


def _cast_to_float(row):
    report = '_pass'
    try:
        row['foo'] = float(row['foo'])
    except TypeError:
        report = row['foo']
    return row, report


# noinspection PyShadowingNames
def test__process__collects_expected_statistics():
    good_rows = [{'foo': i} for i in range(0, 50)]
    bad_rows = [{'foo': None} for i in range(50, 100)]
    new_resources, stats = process([good_rows + bad_rows], _cast_to_float)

    # consume the generators
    list(next(new_resources))

    for i in range(50, 100):
        assert stats[0][i] is None
