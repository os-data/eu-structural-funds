import os
import json
from decimal import Decimal

from datapackage_pipelines.wrapper import ingest, spew

parameters_, datapackage_, resources_ = ingest()

column = parameters_['column']
currency = parameters_['currency']
date_columns = parameters_['date-columns']

#w = open('/Users/adam/keys.txt', 'a')
currencies = json.load(open(os.path.join(os.path.dirname(__file__), 'currencies.json')))

def process(resources):
    def process_single(resource):
        for row in resource:
            ncv = row[column]
            row[column] = None
            if ncv is not None:
                the_date = None
                for date_column in date_columns:
                    the_date = row.get(date_column)
                    if the_date is not None:
                        break
                if the_date is not None:
                    key = "%s-%s" % (currency, the_date.strftime('%Y-%m'))
                    rate = currencies.get(key)
                    if rate is not None:
                        amount = ncv * Decimal(rate)
                        row[column] = amount
                    #w.write(key+'\n')
            yield row

    for resource_ in resources:
        yield process_single(resource_)

spew(datapackage_, process(resources_))

# w.close()