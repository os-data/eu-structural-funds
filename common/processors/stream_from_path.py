"""A processor too stream in data files."""

import logging
import json

from datapackage import DataPackage
from datapackage_pipelines.wrapper import ingest, spew
from petl import fromdicts
from petl import look
from tabulator import Stream

from common.config import DEFAULT_HEADER_LINES, DEFAULT_SAMPLE_SIZE


def remove_empty_headers(headers):
    """Remove columns with no name from the header list."""

    for i, header in enumerate(headers):
        if not header:
            logging.warning('Removing empty header %s', i)
        else:
            yield i, header


def process_with_tabulator(datapackage,
                           encoding=None,
                           parser_options={},
                           header_lines=DEFAULT_HEADER_LINES,
                           format=None):

    for resource in datapackage['resources']:
        with Stream(resource['path'],
                    headers=header_lines,
                    encoding=encoding,
                    format=format,
                    parser_options=parser_options) as stream:

            raw_headers = [' '.join(h.split()) for h in stream.headers]
            i_columns, columns = zip(*list(remove_empty_headers(raw_headers)))
            message = 'Found the following columns: \n%s'
            logging.info(message, json.dumps(raw_headers, indent=4))
            message = 'Ingested the following columns: \n%s'
            logging.info(message, json.dumps(columns, indent=4))

            sample_rows = []

            def process_rows(rows):
                for i, raw_row in enumerate(rows):
                    row = [raw_row[i]
                           for i, column in enumerate(columns)
                           if i in i_columns]
                    row_dict = dict(zip(columns, row))

                    if len(row) != len(columns):
                        row = [v for v in row if v]
                        logging.warning('Bad row %s = %s', i, row)
                        continue

                    if i < DEFAULT_SAMPLE_SIZE:
                        sample_rows.append(row_dict)

                    if i == DEFAULT_SAMPLE_SIZE:
                        table = look(fromdicts(sample_rows),
                                     limit=DEFAULT_SAMPLE_SIZE)
                        logging.info('Content of CSV file is...\n%s', table)

                    yield row_dict

            yield process_rows(stream)


def process_with_datapackage(datapackage):
    for resource in DataPackage(datapackage).resources:
        logging.debug(resource.descriptor)
        logging.debug('Streaming %s', resource.descriptor['path'])
        yield resource.iter()


def stream_local_file(datapackage, **params):
    streamer = params.pop('streamer') if 'streamer' in params else 'tabulator'

    if streamer == 'tabulator':
        return process_with_tabulator(datapackage, **params)
    elif streamer == 'datapackage':
        return process_with_datapackage(datapackage)
    else:
        raise ValueError('%s must be tabulator or datapackage', streamer)


if __name__ == '__main__':
    parameters_, datapackage_, _ = ingest()
    parameters_ = {} if parameters_ is None else parameters_
    resources = stream_local_file(datapackage_, **parameters_)
    spew(datapackage_, resources)
