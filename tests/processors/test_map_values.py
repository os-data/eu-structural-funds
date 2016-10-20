"""Test module for the map_values processor."""

from pytest import mark
from common.processors.map_values import build_lookup_tables, map_aliases

MAPPINGS = [
    {
        'field': 'height',
        'mapping': {
            'TALL': ['huge', 'big'],
            'SHORT': ['small', 'tiny']
        }
    }
]


def test_build_lookup_tables_returns_correct_tables():
    lookup_tables = build_lookup_tables(MAPPINGS)
    assert lookup_tables['height'] == {
        'huge': 'TALL',
        'big': 'TALL',
        'small': 'SHORT',
        'tiny': 'SHORT',
    }


example_rows = [
    ('huge', 'TALL'),
    ('big', 'TALL'),
    ('tiny', 'SHORT'),
    ('small', 'SHORT'),
    ('', None),
    (None, None),
    ('invalid_alias', None)
]


@mark.parametrize('alias, nominal_value', example_rows)
def test_map_aliases_returns_correct_nominal_values(alias, nominal_value):
    lookup_tables = build_lookup_tables(MAPPINGS)
    output_row = map_aliases({'height': alias}, lookup_tables)
    assert output_row['height'] == nominal_value
