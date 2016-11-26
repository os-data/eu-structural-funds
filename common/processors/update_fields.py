"""Map the raw columns names to fiscal fields where indicated."""

import logging
from datapackage_pipelines.wrapper import ingest, spew
from common.utilities import get_fiscal_field_names


def update_datapackage(datapackage):
    valid_fiscal_fields = get_fiscal_field_names()
    for resource in datapackage['resources']:
        for field in resource['schema']['fields']:
            if field['maps_to'] in valid_fiscal_fields:
                field['name'] = field.pop('maps_to')
            else:
                logging.info('Unmapped = %s', field['name'])
    return


_, datapackage_, resources_ = ingest()
spew(update_datapackage(datapackage_), resources_)
