"""Unit-tests for the `sniff_and_cast` processor."""

from datetime import date, datetime
from decimal import Decimal
from math import floor
from jsontableschema.types import DateType, NumberType
from pytest import fixture, mark, raises
from collections import UserList

from common.config import (
    NUMBER_FORMATS,
    DATE_FORMATS,
    SNIFFER_SAMPLE_SIZE,
    SNIFFER_MAX_FAILURE_RATIO
)
from common.processors.sniff_and_cast import (
    extract_data_sample,
    concatenate_data_sample,
    cast_values,
    update_field_types,
    DateSniffer,
    select_sniffer,
    NumberSniffer,
    get_casters,
    CasterNotFound
)


# Sample extraction and concatenation
# -----------------------------------------------------------------------------

@fixture(scope='function')
def resource():
    def generator():
        for i in range(2):
            yield {'foo': i, 'bar': i}

    return generator()


# noinspection PyShadowingNames
def test_extract_data_sample_returns_correct_resource_rows(resource):
    sample, left_over = extract_data_sample(resource, sample_size=1)
    assert sample == [{'foo': 0, 'bar': 0}]
    assert list(left_over) == [{'foo': 1, 'bar': 1}]


# noinspection PyShadowingNames
def test_concatenate_sample_returns_original_resource(resource):
    sample, left_over = extract_data_sample(resource, sample_size=1)
    original_resource = [{'foo': 0, 'bar': 0}, {'foo': 1, 'bar': 1}]
    list(concatenate_data_sample(sample, left_over)) == original_resource


# Row casting
# -----------------------------------------------------------------------------

_CASTERS = {'number': NumberType(), 'date': DateType()}


def test_cast_values_returns_correct_types_on_valid_values():
    good_values_in = {'number': '9,999.99', 'date': '1999-01-01'}
    good_values_out = {'number': Decimal('9999.99'), 'date': date(1999, 1, 1)}
    assert cast_values(good_values_in, _CASTERS) == good_values_out


def test_cast_values_returns_none_and_warns_on_invalid_value(caplog):
    bad_value_in = {'date': 'bad date'}
    bad_value_out = {'date': None}
    assert cast_values(bad_value_in, _CASTERS) == bad_value_out
    assert caplog.records()[0].levelname == 'WARNING'


def test_cast_values_issues_warning_if_data_field_not_in_schema(caplog):
    bad_field_in = {'foo': 'bar'}
    assert cast_values(bad_field_in, _CASTERS, row_index=0) == bad_field_in
    assert caplog.records()[0].levelname == 'WARNING'


def test_cast_values_that_are_already_casted_returns_identical_row():
    casted_values_in = {'date': date(1999, 1, 1), 'number': Decimal(1.2)}
    casted_values_out = {'date': date(1999, 1, 1), 'number': Decimal(1.2)}
    assert cast_values(casted_values_in, _CASTERS,
                       row_index=0) == casted_values_out


# Datapackage mutation
# -----------------------------------------------------------------------------

_ORIGINAL_DATAPACKAGE = {
    'resources': [
        {
            'schema': {
                'fields': [
                    {'name': 'total_amount', 'type': 'type'},
                    {'name': 'starting_date', 'type': 'string'}
                ]
            }
        }
    ]
}

_MUTATED_DATAPACKAGE = {
    'resources': [
        {
            'schema': {
                'fields': [
                    {'name': 'total_amount', 'type': 'number'},
                    {'name': 'starting_date', 'type': 'date'}
                ]
            }
        }
    ]
}


def test_update_field_types_toggles_datapackage_field_types_correctly():
    assert update_field_types(_ORIGINAL_DATAPACKAGE) == _MUTATED_DATAPACKAGE


# Sample generators
# -----------------------------------------------------------------------------

class _BaseGenerator(UserList):
    """A dummy data sample generator."""

    @classmethod
    def load(cls, field_type, format, **kwargs):
        return {
            'date': _DateGenerator,
            'number': _NumberGenerator
        }[field_type](format, **kwargs)

    def __init__(self, format,
                 nb_rows=SNIFFER_SAMPLE_SIZE,
                 nb_bad_rows=0,
                 nb_empty_rows=0):

        self.nb_rows = nb_rows
        self.nb_empty_rows = nb_empty_rows
        self.nb_bad_rows = nb_bad_rows
        self.nb_good_rows = nb_rows - nb_bad_rows - nb_empty_rows
        self.format = format

        super(_BaseGenerator, self).__init__(self.rows)

    @property
    def rows(self):
        for _ in range(self.nb_empty_rows):
            yield {'foo': ''}
        for _ in range(self.nb_bad_rows):
            yield {'foo': 'bad row'}
        for _ in range(self.nb_good_rows):
            yield {'foo': str(self)}


class _NumberGenerator(_BaseGenerator):
    value = 123456.78

    def __str__(self):
        template = '123{groupChar}456{decimalChar}78'
        return template.format(**self.format)


class _DateGenerator(_BaseGenerator):
    value = date(2000, 1, 1)

    def __str__(self):
        template = self.format['format'][4:]
        return datetime(2000, 1, 1).strftime(template)


def _prepare(field_type, format, **kwargs):
    field = {'name': 'foo', 'type': field_type}
    datapackage = {'resources': [{'schema': {'fields': [field]}}]}
    sample_rows = _BaseGenerator.load(field_type, format, **kwargs)
    return datapackage, sample_rows


# Ready-made casters
# -----------------------------------------------------------------------------

def test_select_sniffer_returns_the_correct_sniffer_class():
    assert select_sniffer({'type': 'date'}) == DateSniffer
    assert select_sniffer({'type': 'number'}) == NumberSniffer


# Sniffing success
# -----------------------------------------------------------------------------

@mark.parametrize('format', DATE_FORMATS)
def test_get_caster_returns_valid_caster_from_good_date_samples(format):
    datapackage, sample_rows = _prepare('date', format)
    casters = get_casters(datapackage, sample_rows)
    assert casters['foo'].cast(str(sample_rows)) == sample_rows.value


@mark.parametrize('format', NUMBER_FORMATS)
def test_get_caster_returns_valid_caster_from_good_number_samples(format):
    datapackage, sample_rows = _prepare('number', format)
    casters = get_casters(datapackage, sample_rows)
    result = casters['foo'].cast(str(sample_rows))
    assert abs(float(result) - sample_rows.value) < 0.0001


# Sniffing failure
# -----------------------------------------------------------------------------

_TOO_MANY_BAD_ROWS = floor(SNIFFER_SAMPLE_SIZE * SNIFFER_MAX_FAILURE_RATIO) + 1


@mark.parametrize('format', DATE_FORMATS)
def test_get_caster_raises_caster_not_found_from_bad_date_samples(format):
    datapackage, bad_sample_rows = _prepare('date', format,
                                            nb_bad_rows=_TOO_MANY_BAD_ROWS)
    with raises(CasterNotFound):
        get_casters(datapackage, bad_sample_rows)


@mark.parametrize('format', NUMBER_FORMATS)
def test_get_caster_raises_caster_not_found_from_bad_number_samples(format):
    datapackage, bad_sample_rows = _prepare('number', format,
                                            nb_bad_rows=_TOO_MANY_BAD_ROWS)
    with raises(CasterNotFound):
        get_casters(datapackage, bad_sample_rows)


# Sniffing with empty values in the sample
# -----------------------------------------------------------------------------

@mark.parametrize('format', DATE_FORMATS)
def test_get_caster_fails_with_too_many_empty_sample_values(format):
    datapackage, just_enough_empty_samples_to_fail = _prepare(
        'date', format,
        nb_bad_rows=_TOO_MANY_BAD_ROWS - 1,
        nb_empty_rows=floor(1 / SNIFFER_MAX_FAILURE_RATIO + 1)
    )
    with raises(CasterNotFound):
        get_casters(datapackage, just_enough_empty_samples_to_fail)


# Pre-casting and post-casting checks
# -----------------------------------------------------------------------------

number_field = {
    'name': 'foo',
    'type': 'number',
    'decimalChar': '.',
    'groupChar': ','
}
sample_resources = [
    {'foo': 'bar'},
    {'foo': 'baz'}
]

number_sniffer = NumberSniffer(number_field, sample_resources, 0)


# noinspection PyProtectedMember
def test_has_two_many_decimal_chars():
    assert not number_sniffer._pre_cast_checks_ok('1.2.3')
    assert number_sniffer._pre_cast_checks_ok('1.2')


# noinspection PyProtectedMember
def test_group_char_comes_after_decimal_char():
    assert number_sniffer._pre_cast_checks_ok('1,234.567')
    assert not number_sniffer._pre_cast_checks_ok('1.234,567')


# noinspection PyProtectedMember
def test_has_more_than_two_decimals():
    assert not number_sniffer._post_cast_check_ok(1.233)
    assert number_sniffer._post_cast_check_ok(1.2)


# noinspection PyProtectedMember
def test_pre_and_post_checks_with_corner_cases():
    assert number_sniffer._pre_cast_checks_ok('')
    assert number_sniffer._pre_cast_checks_ok(None)
    assert number_sniffer._post_cast_check_ok(None)
    assert number_sniffer._post_cast_check_ok(1)


# Casting currencies
# -----------------------------------------------------------------------------

_VALID_RAW_CURRENCIES = [
    '1090141.18',
    '€ 1090141.18',
    '€1090141.18',
    '1090141.18 €',
    '1090141.18€',
    '€ 1,090,141.18',
]

currency_field = {
    'name': 'foo',
    'type': 'number',
    'decimalChar': '.',
    'groupChar': ',',
    'format': 'currency'
}

currency_sniffer = NumberSniffer(currency_field, sample_resources, 0)
currency_caster = currency_sniffer.get_caster()


@mark.parametrize('value', _VALID_RAW_CURRENCIES)
def test_number_sniffer_on_values_with_currency_units(value):
    assert currency_caster.cast(value)
