from datapackage_pipelines.wrapper import ingest, spew
import json
import os
import logging

datapackage_file, _, _ = ingest()
source_folder = os.path.dirname(__file__)

logging.debug('Validating %s: %s', source_folder, datapackage_file)
with open(datapackage_file) as stream:
    datapackage = json.loads(stream.read())

spew(datapackage, [])
