"""Grab the source description and convert it into a datapackage"""

import json
import logging

from datapackage_pipelines.wrapper import ingest, spew
from common.config import JSON_FORMAT
from common.utilities import get_fiscal_datapackage



if __name__ == '__main__':
    _, datapackage, resources = ingest()
    fiscal_datapackage = get_fiscal_datapackage(source=datapackage)
    fiscal_datapackage_as_json = json.dumps(fiscal_datapackage, **JSON_FORMAT)
    logging.debug('Loaded fiscal datapackage:\n%s', fiscal_datapackage_as_json)
    spew(fiscal_datapackage, resources)
