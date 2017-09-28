"""A processor to inject constant values into the data."""

from datapackage_pipelines.wrapper import ingest, spew

row_count = 0


def process_rows(prefix, rows):
    global row_count
    for row in rows:
        row['internal_id'] = '{}-{}'.format(prefix, row_count)
        yield row
        row_count += 1


def process(prefix, resources):
    for resource in resources:
        yield process_rows(prefix, resource)


if __name__ == '__main__':
    """Ingest, process and spew out."""

    parameters_, datapackage_, resources_ = ingest()

    spew(datapackage_, process(parameters_['prefix'], resources_))
