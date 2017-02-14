import os
import json
from decimal import Decimal

from datapackage_pipelines.wrapper import ingest, spew

parameters_, datapackage_, resources_ = ingest()

column = parameters_['column']
currency = parameters_['currency']
currency_column = parameters_['currency-column']
date_columns = parameters_['date-columns']

missing = open('missing-keys.txt', 'a')
written = set()
currencies = json.load(open(os.path.join(os.path.dirname(__file__), 'currencies.json')))


def process(resources):
    def process_single(resource):
        for row in resource:
            row[currency_column] = currency
            ncv = row[column]
            row[column] = None
            if ncv is not None:
                the_date = None
                for date_column in date_columns:
                    the_date = row.get(date_column)
                    if the_date is not None:
                        break
                if the_date is not None:
                    keys = ["%s-%s" % (currency, the_date.strftime('%Y-%m'))]
                else:
                    funding_period = list(map(int, row['funding_period'].split('-')))
                    keys = ['%s-%d-06' % (currency, year) for year in range(funding_period[0], funding_period[1])]
                assert len(keys)>0
                all_rates = [(key, currencies.get(key)) for key in keys]
                none_keys = map((lambda x: x[0]),
                                filter((lambda x: x[1] is None), all_rates))
                rates = list(map((lambda x: x[1]),
                                 filter((lambda x: x[1] is not None), all_rates)))
                if len(rates) > 0:
                    rate = sum(rates) / len(rates)
                    amount = ncv * Decimal(rate)
                    row[column] = amount
                for key in none_keys:
                    if key not in written:
                        missing.write(key+'\n')
                        written.add(key)
            yield row

    for resource_ in resources:
        yield process_single(resource_)

for resource in datapackage_['resources']:
    resource['schema']['fields'].append({
        'name': currency_column,
        'type': 'string'
    })

spew(datapackage_, process(resources_))

missing.close()