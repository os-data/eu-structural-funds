"""A processor too stream in data files."""

import os
import json

import cchardet as chardet
from tabulator import Stream
from logging import warning, info
from datapackage_pipelines.wrapper import ingest, spew
from petl import look, fromdicts
from common.config import SAMPLE_SIZE
from common.utilities import format_to_json


def get_json_headers(path):
    """Return all field names encountered in the file."""

    keys = set()

    with open(path) as stream:
        rows = json.loads(stream.read())
        for row in rows:
            keys.update(set(row))
        return list(keys)


def detect_encoding(path):
    """Detect the data file encoding with Chardet."""

    with open(path, 'rb') as stream:
        text = stream.read()
    result = chardet.detect(text)
    return result['encoding']


def drop_bad_rows(rows):
    """Drop rows where fields don't match headers."""

    for index, headers, row in rows:
        if len(row) == len(headers):
            yield (index, headers, row)
        else:
            warning('Bad row %s = %s', index, row)


def drop_empty_columns(rows):
    """Drop fields with no name."""

    for row_nb, headers, row in rows:
        for field_nb, header in enumerate(headers):
            if not header:
                headers.remove(field_nb)
                row.remove(field_nb)
            if row_nb < SAMPLE_SIZE:
                message = 'Dropped untitled field %s in row %s'
                warning(message, field_nb, row_nb)

        yield (row_nb, headers, row)


def fill_missing_fields(path):
    """Pre-fill incomplete JSON rows (necessary to avoid mixing up fields)."""

    headers = get_json_headers(path)

    with open(path) as stream:
        rows = json.loads(stream.read())

    for row in rows:
        for header in headers:
            if header not in row:
                row[header] = None

    with open(path, 'w+') as stream:
        stream.write(format_to_json(rows))


def log_sample_table(stream):
    """Record a tabular representation of the stream sample to the log."""

    samples = list(map(lambda x: dict(zip(stream.headers, x)), stream.sample))
    table = str(look(fromdicts(samples), limit=len(stream.sample)))
    info('Data sample =\n%s', table)


def check_fields_match(resource, stream):
    """Check if the datapackage and the data have the same set of fields."""

    data_fields = set(stream.headers)
    sourced_fields = {field['name'] for field in resource['schema']['fields']}

    info('Fields sourced = %s', format_to_json(list(sourced_fields)))
    info('Fields in data = %s', format_to_json(list(data_fields)))

    message = 'Data and source fields do not match'
    assert data_fields == sourced_fields, message


def stream_local_file(datapackage, **parameters):
    """Read local files and return row iterators."""

    if not parameters.get('sample_size'):
        parameters.update(sample_size=SAMPLE_SIZE)

    for resource in datapackage['resources']:
        path = resource['path']
        _, extension = os.path.splitext(path)

        if not parameters.get('encoding'):
            parameters.update(encoding=detect_encoding(path))

        if 'parser_options' in resource:
            parameters.update(**resource.get('parser_options'))

        if extension == '.csv':
            parameters.update(headers=1)
            parameters.update(post_parse=[drop_bad_rows])

        if extension == '.json':
            fill_missing_fields(path)
            parameters.update(headers=1)

        info('Ingesting file = %s', path)
        info('Ingestion parameters = %s', format_to_json(parameters))

        with Stream(path, **parameters) as stream:
            check_fields_match(resource, stream)
            log_sample_table(stream)
            yield stream.iter(keyed=True)


if __name__ == '__main__':
    parameters_, datapackage_, _ = ingest()
    parameters_ = {} if parameters_ is None else parameters_
    resources = stream_local_file(datapackage_, **parameters_)
    spew(datapackage_, resources)
