"""A processor to add constants.

Assumptions
-----------

The processor assumes that the data conforms to the fiscal schema.

Data processing
---------------

The processor adds the following information:

    - beneficiary_country_code
    - beneficiary_country
    - beneficiary_nuts_code
    - beneficiary_nuts_region

Datapackage mutation
--------------------

None.

"""

import os

from datapackage_pipelines.wrapper import ingest, spew

from common.utilities import process


def add_geocodes(row, **kw):
    """Fill up the country and region fields."""

    row['beneficiary_country_code'] = kw['country_code']
    row['beneficiary_country'] = kw['country']
    row['beneficiary_nuts_code'] = kw['nuts_code']
    row['beneficiary_nuts_region'] = kw['region']

    return row


if __name__ == '__main__':
    parameters_, datapackage, resources = ingest()

    new_resources = process(resources, add_geocodes,
                            **parameters_)
    spew(datapackage, new_resources)
