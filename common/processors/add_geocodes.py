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
from common.bootstrap import Source


def add_geocodes(row, source):
    """Fill up the country and region fields."""

    row['beneficiary_country_code'] = source.country_code
    row['beneficiary_country'] = source.country
    row['beneficiary_nuts_code'] = source.nuts_code
    row['beneficiary_nuts_region'] = source.region

    return row


if __name__ == '__main__':
    _, datapackage, resources = ingest()
    new_resources = process(resources, add_geocodes,
                            source=Source(folder=os.getcwd()))
    spew(datapackage, new_resources)
