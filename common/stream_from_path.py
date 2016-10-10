"""A processor too stream in data files."""

import logging

from datapackage import DataPackage
from datapackage_pipelines.wrapper import ingest, spew
from tabulator import Stream


# from datapackage import Resource


def process_with_tabulator(datapackage):
    for resource in datapackage['resources']:
        with Stream(resource['path'], headers=1) as table:
            def process_rows(rows):
                for row in rows:
                    yield dict(zip(table.headers, row))

            yield process_rows(table)


def process_with_datapackage(datapackage):
    for resource in DataPackage(datapackage).resources:
        logging.debug(resource.descriptor)
        logging.debug('Streaming %s', resource.descriptor['path'])
        yield resource.iter()


def stream_local_file(datapackage, streamer):
    if streamer == 'tabulator':
        return process_with_tabulator(datapackage)
    elif streamer == 'datapackage':
        return process_with_datapackage(datapackage)
    else:
        raise ValueError('%s is must be tabulator or datapackage', streamer)


if __name__ == '__main__':
    parameters_, datapackage_, _ = ingest()
    streamer_ = parameters_['streamer']
    resources = stream_local_file(datapackage_, streamer_)
    spew(datapackage_, resources)
