import fingerprints

from datapackage_pipelines.wrapper import ingest, spew

parameters_, datapackage_, resources_ = ingest()


def process_single(resource):
    for row in resource:
        fp = fingerprints.generate(row['beneficiary_name'])
        if fp is not None:
            row['beneficiary_id'] = fp.capitalize()
        else:
            row['beneficiary_id'] = row['beneficiary_name']
        yield row


def process(resources):
    for resource_ in resources:
        yield process_single(resource_)


spew(datapackage_, process(resources_))

