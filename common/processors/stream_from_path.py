"""A processor to stream data from files."""

import os
import json

import cchardet as chardet
from tabulator import Stream
from logging import warning, info
from datapackage_pipelines.wrapper import ingest, spew
from petl import look, fromdicts
from common.config import SAMPLE_SIZE
from common.utilities import format_to_json, sanitize_field_names


def get_json_headers(path):
    """Return all field names encountered in the file."""

    keys = set()

    with open(path) as stream:
        rows = json.loads(stream.read())
        for row in rows:
            keys.update(set(row))
        return list(keys)


def get_encoding(parameters, resource):
    """Return either the specified encoding or a best guess."""

    with open(resource['path'], 'rb') as stream:
        text = stream.read()
        encoding = chardet.detect(text)['encoding']

    if parameters.get('encoding'):
        encoding = parameters.get('encoding')
    if resource.get('encoding'):
        encoding = resource.get('encoding')
    return encoding


def drop_bad_rows(rows):
    """Drop rows where fields don't match headers."""

    for index, headers, row in rows:
        if len(row) == len(headers):
            yield (index, headers, row)
        else:
            warning('Bad row %s = %s', index, row)


def drop_empty_columns(rows):
    """Drop fields with no name."""

    for index, headers, values in rows:
        valid_headers = [header for header in headers if header]
        valid_values = [value for i, value in enumerate(values) if headers[i]]

        if index < SAMPLE_SIZE:
            nb_empty_headers = len(headers) - len(valid_headers)

            if nb_empty_headers > 0:
                message = 'Dropped %s columns from row %s'
                warning(message, nb_empty_headers, index)

        yield (index, valid_headers, valid_values)


def force_strings(rows):
    """Force all fields to strings."""

    for index, headers, values in rows:
        values_as_strings = list(map(str, values))
        yield (index, headers, values_as_strings)


def fill_missing_fields(path):
    """Pre-fill incomplete JSON rows (to avoid fields mixing up)."""

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
    table = look(fromdicts(samples), limit=len(stream.sample))
    info('Data sample =\n%s', table)


def check_fields_match(resource, stream):
    """Check if the datapackage and the data have the same set of fields."""

    data_fields = [str(field) for field in stream.headers if field]
    sourced_fields = [field['name'] for field in resource['schema']['fields']]
    nb_untitled_fields = len(stream.headers) - len(data_fields)

    fields_as_json = format_to_json(sorted(sourced_fields))
    data_fields_as_json = format_to_json(sorted(data_fields))

    info('%s fields sourced = %s', len(sourced_fields), fields_as_json)
    info('%s untitled fields in the data', nb_untitled_fields)
    info('%s fields in the data = %s', len(data_fields), data_fields_as_json)

    message = 'Data and source fields do not match'
    assert set(data_fields) == set(sourced_fields), message


def get_headers(parameters, path):
    """Return a list of cleaned up headers."""

    with Stream(path, **parameters) as stream:
        return sanitize_field_names(stream.headers)


def stream_local_file(datapackage, **parameters):
    """Read local files and return row iterators."""

    if not parameters.get('sample_size'):
        parameters.update(sample_size=SAMPLE_SIZE)

    for resource in datapackage['resources']:
        path = resource['path']
        _, extension = os.path.splitext(path)

        parameters.update(headers=1)

        if 'parser_options' in resource:
            parameters.update(**resource.get('parser_options'))

        if extension == '.csv':
            parameters.update(post_parse=[drop_bad_rows, drop_empty_columns])
            parameters.update(encoding=get_encoding(parameters, resource))

        if extension in ('.xls', '.xlsx'):
            parameters.update(post_parse=[drop_empty_columns, force_strings])

        if extension == '.json':
            fill_missing_fields(path)
            parameters.update(post_parse=[force_strings])

        info('Ingesting file = %s', path)
        info('Ingestion parameters = %s', format_to_json(parameters))

        parameters.update(headers=get_headers(parameters, path))

        with Stream(path, **parameters) as stream:
            check_fields_match(resource, stream)
            log_sample_table(stream)
            yield stream.iter(keyed=True)


if __name__ == '__main__':
    parameters_, datapackage_, _ = ingest()
    parameters_ = {} if parameters_ is None else parameters_
    resources = stream_local_file(datapackage_, **parameters_)
    spew(datapackage_, resources)
