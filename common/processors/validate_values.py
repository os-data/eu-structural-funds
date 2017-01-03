"""Map the raw columns names to fiscal fields where indicated."""

import logging
from datapackage_pipelines.wrapper import ingest, spew
from common.utilities import get_fiscal_field_names


parameters_, datapackage_, resources_ = ingest()

thresholds = parameters_['thresholds']
columns = thresholds.keys()

counter = 0
nones = dict((c,0) for c in columns)

def is_empty(value):
    if value is None: return True
    if type(value) is str and value.strip()=='': return True
    return False

def process(resources):
    def process_single(resource):
        global counter, nones
        for row in resource:
            counter += 1
            for column in columns:
                value = row.get(column)
                if is_empty(value):
                    nones[column] += 1
            yield row
    for resource_ in resources:
        yield process_single(resource_)

spew(datapackage_, process(resources_))

for column in columns:
    ratio_percent = 100 - (100*nones[column])//counter
    if ratio_percent < thresholds[column]:
        raise ValueError('%s: Got %d empty values (out of %d), which is %d%% (below the threshold of %d%%)' %
                         (column, nones[column], counter, ratio_percent, thresholds[column]) )
