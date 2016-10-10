"""Grab the source description and convert it into a datapackage"""

import logging

from datapackage_pipelines.wrapper import ingest, spew
from plumbing.utilities import get_fiscal_datapackage

if __name__ == '__main__':
    # noinspection PyRedeclaration
    _, _, resources_ = ingest()
    datapackage_ = get_fiscal_datapackage()
    logging.debug('Datapackage = %s', datapackage_)
    spew(datapackage_, resources_)
