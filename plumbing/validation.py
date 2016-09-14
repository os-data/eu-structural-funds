SOURCE = 'https://raw.githubusercontent.com/okfn/jsontableschema-py/master/data/data_infer.csv'
SCHEMA = {'fields': [{'name': 'id', 'type': 'integer'}, {'name': 'age', 'type': 'integer'}, {'name': 'name', 'type': 'string'}] }


# If schema is not passed it will be inferred
table = Table(SOURCE, schema=SCHEMA)
rows = table.iter()
while True:
   try:
       print(next(rows))
   except StopIteration:
       break
   except Exception as exception:
       print(exception)
# evgenykarev
# 4:12 PM @loic the way I'm going to use in goodtables.next (or something like this):

schema = jsontableschema.Schema(descriptor)
def validate(extended_rows):
   for number, headers, row in extended_rows:
       # here you have also schema.headers and schema.get_field('name').convert
       # so any granular checks could be done

tabulator.topen(source, headers=1, post_parse=[validate]).read()