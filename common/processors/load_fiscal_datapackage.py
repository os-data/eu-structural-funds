"""Grab the source description and convert it into a datapackage"""

import logging

from datapackage_pipelines.wrapper import ingest, spew
from common.utilities import get_fiscal_datapackage

if __name__ == '__main__':
    # noinspection PyRedeclaration
    _, datapackage, resources = ingest()
    fiscal_datapackage = get_fiscal_datapackage(source=datapackage)
    logging.debug('Datapackage = %s', datapackage)
    spew(fiscal_datapackage, resources)
