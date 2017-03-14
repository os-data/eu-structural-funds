"""Map the raw columns names to fiscal fields where indicated."""

from datapackage_pipelines.wrapper import ingest, spew

parameters_, datapackage_, resources_ = ingest()

thresholds = parameters_['thresholds']
allowed_values = parameters_['allowed_values']
threshold_columns = thresholds.keys()
allowed_value_columns = allowed_values.keys()


def is_empty(value):
    if value is None: return True
    if type(value) is str and value.strip()=='': return True
    return False


def process(resources):
    def process_single(resource):
        counter = 0
        nones = dict((c,0) for c in threshold_columns)
        for row in resource:
            counter += 1
            for column in threshold_columns:
                value = row.get(column)
                if is_empty(value):
                    nones[column] += 1
            for column in allowed_value_columns:
                value = row.get(column)
                if not is_empty(value) and value != 'unknown':
                    if value not in allowed_values[column]:
                        raise ValueError('%s: Got %r whereas allowed values for this column are %r' %
                                         (column, value, allowed_values[column]))
            yield row
        for column in threshold_columns:
            ratio_percent = 100 - (100*nones[column])//counter
            if ratio_percent < thresholds[column]:
                raise ValueError('%s: Got %d good values (out of %d), which is %d%% (below the threshold of %d%%)' %
                                 (column, counter-nones[column], counter, ratio_percent, thresholds[column]) )

    for resource_ in resources:
        yield process_single(resource_)

spew(datapackage_, process(resources_))

