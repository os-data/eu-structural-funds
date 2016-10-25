"""A processor too stream in data files."""

import logging

from datapackage import DataPackage
from datapackage_pipelines.wrapper import ingest, spew
from tabulator import Stream

from common.config import DEFAULT_HEADERS


def process_with_tabulator(datapackage,
                           encoding=None,
                           parser_options={},
                           headers=DEFAULT_HEADERS,
                           format=None):

    for resource in datapackage['resources']:
        with Stream(resource['path'],
                    headers=headers,
                    encoding=encoding,
                    format=format,
                    parser_options=parser_options) as table:

            def process_rows(rows):
                for i, row in enumerate(rows):
                    row_dict = dict(zip(table.headers, row))
                    if i < 10:
                        logging.debug('Row %s = %s', i, row_dict)
                    yield row_dict

            yield process_rows(table)


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
