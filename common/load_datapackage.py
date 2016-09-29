"""Grab the source description and convert it into a datapackage"""

import json
import logging

from datapackage_pipelines.wrapper import ingest, spew

# noinspection PyRedeclaration
parameters, _, _ = ingest()

with open(parameters['datapackage_file']) as stream:
    datapackage = json.loads(stream.read())

logging.debug('Ingested description file: %s', datapackage['title'])
spew(datapackage, [])
