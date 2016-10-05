import logging
import petl
import json

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, input_resources = ingest()
output_resources = []

logging.debug('Source: %s', datapackage['title'])


def get_iterators():
    for resource in datapackage['resources']:
        # for some reason the xls file is invalid tp petl
        # source = petl.URLSource(resource['url'])
        # table = petl.fromxls(source)

        logging.debug('Resource: %s', resource['title'])
        table = petl.fromxls(
            '/home/loic/repos/eu-structural-funds/data/FR.france/2014-2020/20160711_listeoperations_FEDER_FSE_14-20.xls')
        headers = table.header()
        rows = petl.dicts(table)

        def get_iterator(rows_):
            for row in rows_:
                row_dict = dict(zip(headers, map(str, row)))
                yield json.dumps(row_dict) + '\n'

        yield get_iterator(rows)


spew(datapackage, get_iterators())
