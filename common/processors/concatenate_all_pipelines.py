"""This processor concatenates all datasets into one."""

import yaml

from logging import warning, info

from datapackage_pipelines.wrapper import ingest
from petl import fromdicts, look
from tabulator import Stream
from zipfile import BadZipFile, ZipFile
from datapackage_pipelines.wrapper import spew

from common.utilities import format_to_json
from common.bootstrap import collect_sources
from common.config import (
    FISCAL_METADATA_FILE,
    FISCAL_SCHEMA_FILE,
    FISCAL_MODEL_FILE,
    DATAPACKAGE_FILE,
    SAMPLE_SIZE)


STREAM_OPTIONS = {
    'scheme': 'text',
    'format': 'csv',
    'encoding': 'utf-8',
    'headers': 1,
    'sample_size': SAMPLE_SIZE
}


def format_as_table(stream):
    """Convert extended rows to a petl table."""

    keyed_rows = []

    for row in stream.sample:
        keyed_rows.append(dict(zip(stream.headers, row)))

    return repr(look(fromdicts(keyed_rows), limit=None))


def concatenate(select=None):
    """Return one generator of rows for all datasets."""

    for source in collect_sources(select):
        if source.fiscal_zip_file:
            try:
                with ZipFile(source.fiscal_zip_file) as zipped_files:
                    csv_files = zipped_files.namelist()
                    csv_files.remove(DATAPACKAGE_FILE)

                    for i, csv_file in enumerate(csv_files):
                        csv_text = zipped_files.read(csv_file).decode()

                        with Stream(csv_text, **STREAM_OPTIONS) as stream:
                            message = 'Concatenating resource %s of %s...'
                            info(message, i, csv_file)

                            for row in stream.iter(keyed=True):
                                yield row

                            message = 'Done. File sample:\n%s'
                            info(message, format_as_table(stream))

            except BadZipFile:
                warning('%s has a bad zip file', source.id)


def describe():
    """Assemble the fiscal datapackage for the concatenated dataset."""

    with open(FISCAL_METADATA_FILE) as stream:
        fdp = yaml.load(stream.read())

    with open(FISCAL_MODEL_FILE) as stream:
        fdp['model'] = yaml.load(stream.read())

    with open(FISCAL_SCHEMA_FILE) as stream:
        fdp['resources'][0]['schema'] = yaml.load(stream.read())

    message = 'Fiscal datapackage: \n%s'
    info(message, format_to_json(fdp))

    return fdp


if __name__ == '__main__':
    parameters, datapackage, _ = ingest()
    resources = concatenate(select=parameters)
    datapackage = describe()
    spew(datapackage, [resources])
