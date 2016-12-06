"""Unit-tests for the `add_geocodes` processor."""

from tests.test_bootstrap import TestGeoProperties
from common.processors.add_geocodes import add_geocodes


class TestAddGeoCodes(TestGeoProperties):
    def test_add_geocodes_fills_geographical_information(self):
        input_row = {
            'beneficiary_country_code': None,
            'beneficiary_country': None,
            'beneficiary_nuts_code': None,
            'beneficiary_nuts_region': None
        }
        output_row = {
            'beneficiary_country_code': 'XX',
            'beneficiary_country': 'NUTS LEVEL 1',
            'beneficiary_nuts_code': 'XX',
            'beneficiary_nuts_region': 'NUTS LEVEL 1'
        }
        self.assertEquals(add_geocodes(input_row, self.source), output_row)
