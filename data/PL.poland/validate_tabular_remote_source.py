""" A processor to download a data-file from a link and validate it."""

from os.path import dirname, join

from requests import get
from slugify import slugify
from yaml import load
from json import dumps
from jsontableschema
from datapackage import DataPackage, pull_datapackage
from logging import debug

from datapackage_pipelines.wrapper import ingest, spew


CHUNK_SIZE = 1024
LOCAL_FOLDER = dirname(__file__)
DESCRIPTION_FILEPATH = join(LOCAL_FOLDER, 'source.description.yaml')
DATAPACKAGE_FILEPATH = join(LOCAL_FOLDER, 'source.datapackage.json')


# def download_file(url):
#     local_filename = url.split('/')[-1]
#     r = get(url, stream=True)
#     with open(local_filename, 'wb') as f:
#         for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
#             if chunk:
#                 f.write(chunk)
#     return local_filename



def validate_data():
    """Validate the data-files against a pre-defined schema."""

    params, datapackage, resources = ingest()

    descriptor = get_descriptor()
    source_schema = DataPackage(descriptor)
    for resource in resources:


    # If schema is not passed it will be inferred
    table = Table(SOURCE, schema=source_schema)
    rows = table.iter()
    while True:
       try:
           print(next(rows))
       except StopIteration:
           break
       except Exception as exception:
           print(exception)


def get_descriptor():
    """Assembles the datapackage that describes the data source."""

    with open(DESCRIPTION_FILEPATH) as yaml:
        descriptor = load(yaml.read())

    descriptor['name'] = slugify('title', separator='_')

    with open(DATAPACKAGE_FILEPATH, 'w+') as json:
        json.write(dumps(descriptor, indent=4))

    return descriptor


if __name__ == "__main__":
    validate_data()

