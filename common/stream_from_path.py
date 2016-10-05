import logging

from datapackage import DataPackage
from datapackage_pipelines.wrapper import ingest, spew

_, datapackage_descriptor, _ = ingest()


def generate_resources():
    datapackage = DataPackage(datapackage_descriptor)
    for resource in datapackage.resources:
        logging.debug(resource.descriptor)
        logging.debug('Streaming %s', resource.descriptor['path'])
        yield resource.iter()


spew(datapackage_descriptor, generate_resources())
