"""This processor concatenates all datasets into one."""

import yaml
import logging

from tabulator import Stream
from zipfile import BadZipFile, ZipFile
from datapackage_pipelines.wrapper import spew

from common.utilities import format_to_json
from common.bootstrap import collect_sources
from common.config import (
    FISCAL_DATAPACKAGE_FILE,
    FISCAL_SCHEMA_FILE,
    FISCAL_MODEL_FILE,
    DATAPACKAGE_FILE
)

STREAM_OPTIONS = {
    'scheme': 'text',
    'format': 'csv',
    'encoding': 'utf-8',
    'headers': 1
}


def concatenate():
    """Return one generator of rows for all datasets."""

    for source in collect_sources():
        if source.fiscal_zip_file:
            try:
                with ZipFile(source.fiscal_zip_file) as zipfile:
                    csv_files = zipfile.namelist()
                    csv_files.remove(DATAPACKAGE_FILE)

                    for i, csv_file in enumerate(csv_files):
                        csv_text = zipfile.read(csv_file).decode()

                        with Stream(csv_text, **STREAM_OPTIONS) as rows:
                            for row in rows.iter(keyed=True):
                                yield row

                            message = 'Concatenated resource %s = %s'
                            logging.info(message, i, csv_file)

            except BadZipFile:
                logging.warning('%s has a bad zip file', source.id)


def describe():
    """Return the description of the fiscal datapackage."""

    with open(FISCAL_DATAPACKAGE_FILE) as stream:
        fdp = yaml.load(stream.read())

    with open(FISCAL_MODEL_FILE) as stream:
        fdp['model'] = yaml.load(stream.read())

    with open(FISCAL_SCHEMA_FILE) as stream:
        fdp['resources'][0]['schema'] = yaml.load(stream.read())

    message = 'Fiscal datapackage: \n%s'
    logging.info(message, format_to_json(fdp))

    return fdp


if __name__ == '__main__':
    resources = concatenate()
    datapackage = describe()
    spew(datapackage, [resources])
