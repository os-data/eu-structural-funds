"""This processor concatenates all datasets into one."""

import yaml

from logging import warning, info
from datapackage_pipelines.wrapper import ingest
from petl import fromdicts, look
from tabulator import Stream
from zipfile import BadZipFile, ZipFile
from datapackage_pipelines.wrapper import spew

from common.utilities import format_to_json, get_fiscal_field_names
from common.bootstrap import collect_sources
from common.config import (
    FISCAL_METADATA_FILE,
    FISCAL_SCHEMA_FILE,
    FISCAL_MODEL_FILE,
    DATAPACKAGE_FILE,
    LOG_SAMPLE_SIZE,
    DATA_DIR
)


STREAM_OPTIONS = {
    'scheme': 'text',
    'format': 'csv',
    'encoding': 'utf-8',
    'headers': 1,
    'sample_size': LOG_SAMPLE_SIZE
}


def format_data_sample(stream):
    """Return a table representation of a sample of the streamed data."""

    keyed_rows = []
    for row in stream.sample:
        keyed_rows.append(dict(zip(stream.headers, row)))

    petl_table = fromdicts(keyed_rows)
    return repr(look(petl_table, limit=None))


def collect_local_datasets(**params):
    """Collect all local fiscal datasets."""

    for source in collect_sources(select=params.get('pipelines')):
        if source.fiscal_zip_file:
            info('Found %s', source.fiscal_zip_file.lstrip(DATA_DIR))

            try:
                with ZipFile(source.fiscal_zip_file) as zipped_files:
                    filenames = zipped_files.namelist()
                    filenames.remove(DATAPACKAGE_FILE)

                    for i, filename in enumerate(filenames):
                        csv_bytes = zipped_files.read(filename)
                        csv_text = csv_bytes.decode()

                        yield filename, csv_text

                        message = 'Collected resource %s of %s: %s'
                        info(message, i, source.id, filename)

            except BadZipFile:
                warning('%s has a bad zip file', source.id)


def concatenate(csv_files, **params):
    """Return a single resource generator for all datasets."""

    fiscal_fields = get_fiscal_field_names()
    fields_subset = params.get('fields') or fiscal_fields
    if not (set(fields_subset) <= set(fiscal_fields)):
        raise ValueError('Invalid subset of fields')

    nb_files = 0
    for filename, csv_text in csv_files:
        nb_files += 1

        with Stream(csv_text, **STREAM_OPTIONS) as stream:
            for row in stream.iter(keyed=True):
                yield {key: value
                       for key, value in row.items()
                       if key in fields_subset}

            args = filename, format_data_sample(stream)
            info('Concatenated %s:\n%s', *args)

    info('Done concatenating %s files', nb_files)


def assemble_fiscal_datapackage():
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
    datapackage = assemble_fiscal_datapackage()
    datasets = collect_local_datasets(**parameters)
    resource = concatenate(datasets, **parameters)
    spew(datapackage, [resource])
