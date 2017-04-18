"""Map the raw columns names to fiscal fields where indicated."""
import re

from datapackage_pipelines.wrapper import ingest, spew

parameters_, datapackage_, resources_ = ingest()

column_order = parameters_['column-order']
target_column = parameters_['target-column']
kind_column = parameters_['kind-column']

from_one = re.compile('[1-9]+')
from_zero = re.compile('[0-9]+')


def is_empty(value, digits):
    if value is None: return True
    value = str(value).strip()
    if value == '': return True
    if len(digits.findall(value)) == 0: return True
    return False


def process(resources):
    def process_single(resource):
        for row in resource:
            for digits in [from_one, from_zero]:
                done = False
                for column in column_order:
                    if is_empty(row.get(column), digits):
                        continue
                    row[target_column] = row[column]
                    row[kind_column] = column
                    done = True
                    break
                if done: break
            yield row

    for resource_ in resources:
        yield process_single(resource_)


# for resource in datapackage_['resources']:
#     fields = resource.get('schema', {}).get('fields', [])
#     data_type = [f['type'] for f in fields if f['name'] == column_order[0]][0]
#     fields.append({
#         'name': target_column,
#         'type': data_type
#     })
#     fields.append({
#         'name': kind_column,
#         'type': 'string'
#     })

spew(datapackage_, process(resources_))

