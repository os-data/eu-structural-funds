"""This processor shows first 5 lines of the data in the console."""

import logging
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.wrapper import spew

DEFAULT_SAMPLE_SIZE = 10


def get_row_subset(row, fields):
    key_subset = fields or row.keys()
    for key in row:
        if key in key_subset:
            yield key, row[key]


def process_resources(resources, sample_size, fields):
    """Return an iterator of row iterators."""

    for resource in resources:
        def process_row(resource_, ):
            for i, row in enumerate(resource_):
                if i < sample_size:
                    row_subset = get_row_subset(row, fields)
                    logging.info('Row %s = %s', i, dict(row_subset))
                yield row

        yield process_row(resource)


if __name__ == '__main__':
    parameters, datapackage_, resources_ = ingest()
    sample_size_ = parameters.get('sample_size', DEFAULT_SAMPLE_SIZE)
    fields_ = parameters.get('fields')
    new_resources_ = process_resources(resources_, sample_size_, fields_)
    spew(datapackage_, new_resources_)
