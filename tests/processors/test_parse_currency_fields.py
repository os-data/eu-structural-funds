"""Test module for the parse_currency_fields processor."""

from pytest import mark
from common.processors.parse_currency_fields import parse_currencies

parsing_examples = [
    ('1,234.56€', (',', '.', '€'), 1234.56),
    ('€1,234.56', (',', '.', '€'), 1234.56),
    ('€ 1,234.56', (',', '.', '€'), 1234.56),
    ('1,234.56 €', (',', '.', '€'), 1234.56),
    ('1.234,56€  ', ('.', ',', '€'), 1234.56),
    ('   1.234,56€', ('.', ',', '€'), 1234.56),
    ('   1.234,56   ', ('.', ',', '€'), 1234.56),
    ('1.234,56', ('.', ',', '€'), 1234.56),
    ('', ('.', ',', '€'), None),
    ('', ('.,', '.', 'foo'), None)
]


@mark.parametrize('raw_string, character_values, result', parsing_examples)
def test_parse_currencies(raw_string, character_values, result):
    character_keys = ('grouping', 'decimal', 'currency')
    characters = zip(character_keys, character_values)
    args = {'amount': raw_string}, ['amount'], dict(characters)
    assert parse_currencies(*args)['amount'] == result
