import os
import sys
import hashlib

import json
import yaml
import slugify

from .config import SOURCE_FILE, PIPELINE_FILE, DATA_DIR, FISCAL_SCHEMA_FILE

PREPROCESSING = {
    'parse_currency_fields',
    'convert_excel_dates',
    'MT.parse_dates',
    'NL.parse_dates'
}

if __name__ == "__main__":

    fiscal_schema = yaml.load(open(FISCAL_SCHEMA_FILE))

    update = False
    if len(sys.argv) > 1:
        if sys.argv[1] == 'update':
            update = True

    for dirpath, dirnames, filenames in os.walk(DATA_DIR):
        if SOURCE_FILE in filenames:
            source = yaml.load(open(os.path.join(dirpath, SOURCE_FILE)))
            if update:
                try:
                    current = yaml.load(open(os.path.join(dirpath, PIPELINE_FILE)))
                except:
                    continue
                current = list(current.values())[0]['pipeline']

            try:
                preprocessing = yaml.load(open(os.path.join(dirpath, 'preprocessing.yaml')))
            except:
                preprocessing = []
            try:
                mappings = yaml.load(open(os.path.join(dirpath, 'mappings.yaml')))
            except:
                mappings = {'mappings': []}

            fiscal_model_parameters = {
                'options': dict(
                    (field['name'], {'currency': source['resources'][0].get('currency_code', 'EUR')})
                    for field in fiscal_schema['fields']
                    if field.get('osType') == 'value'
                ),
                'os-types': dict(
                    (field['name'], field['osType'])
                    for field in fiscal_schema['fields']
                    if 'osType' in field
                ),
                'titles': dict(
                    (field['name'], field['title'])
                    for field in fiscal_schema['fields']
                    if 'title' in field
                )
            }

            concat_parameters = dict(
                (field['name'], []) for field in fiscal_schema['fields']
            )
            for resource in source['resources']:
                schema = resource.get('schema')
                if schema is not None:
                    for field in schema.get('fields',[]):
                        maps_to = field.get('maps_to')
                        if maps_to is not None and not maps_to.startswith('_'):
                            aliases = concat_parameters[maps_to]
                            if field['name'] not in aliases:
                                aliases.append(field['name'])
                            del field['maps_to']
                            if 'translates_to' in field:
                                del field['translates_to']
            concat_parameters = {'column-aliases': concat_parameters}
            sniff_and_cast_parameters = {}
            if source.get('currency_symbol') is not None:
                sniff_and_cast_parameters['currency_symbol'] = source['currency_symbol']

            pipeline = [
                ('read_description', {'datapackage': source}),
                ('ingest_local_file', {}),
                ('map_values', mappings),
                # ('map_fields', {}),
                # ('concatenate_identical_resources', {})
                ('concat', concat_parameters),
            ] + preprocessing + [
                ('reshape_data', {}),
                ('show_sample_in_console', {'sample_size': 10}),
                ('add_geocodes', {}),
                ('add_constants', {}),
                ('add_categories', {}),
                ('fiscal.model', fiscal_model_parameters),
                ('sniff_and_cast', sniff_and_cast_parameters),
                ('dump', {'out-file': 'fiscal.datapackage.zip'}),
                ('fiscal.upload', {'in-file': 'fiscal.datapackage.zip'}),
            ]
            orig_pipeline = pipeline[:]

            if update:
                print()
                for item in current:
                    try:
                        if item['run'] == 'map_values':
                            yaml.dump(item['parameters'], open(os.path.join(dirpath, 'mappings.yaml'), 'w'))

                        if item['run'] == pipeline[0][0]:
                            if (json.dumps(item.get('parameters', {}), sort_keys=True) !=
                                    json.dumps(pipeline[0][1], sort_keys=True)):
                                if item['run'] in {'read_description'}:
                                    pass
                                else:
                                    print(dirpath)
                                    print('PARAM MISMATCH', item, pipeline[0][1])
                            # print(item['run'],'==',pipeline[0][0])
                            pipeline.pop(0)
                        else:
                            while item['run'] != pipeline[0][0]:
                                # print(item['run'],'!=',pipeline[0][0])
                                if item['run'] in {'dump',
                                                   'show_sample_in_console',
                                                   'stream_remote_excel',
                                                   'mutate_datapackage',
                                                   'map_columns',
                                                   'cast_to_fiscal_schema',
                                                   'nop'}:
                                    break
                                elif item['run'] in {'concatenate_identical_resources',
                                                     'reshape_data',
                                                     'add_geocodes',
                                                     'add_constants',
                                                     'add_categories',
                                                     }:
                                    pipeline_item = list(filter(lambda x:x[0] == item['run'], orig_pipeline))[0]
                                    assert(len(item.get('parameters', {}))==0)
                                    break
                                elif item['run'] in PREPROCESSING:
                                    if item['run'] not in set(x[0] for x in preprocessing):
                                        preprocessing.append((item['run'], item.get('parameters', {})))
                                        yaml.dump(preprocessing, open(os.path.join(dirpath, 'preprocessing.yaml'), 'w'))
                                    break
                                pipeline.pop(0)
                    except Exception as e:
                        print(e)
                        print(dirpath)
                        print('EXTRA', item)
                        sys.exit(0)

            pipeline = {
                'schedule': {'crontab': '0 0 1 1 *'},
                'pipeline': [
                    {
                        'run': x[0],
                        'parameters': x[1]
                    }
                    for x in orig_pipeline
                ]
            }
            title = slugify.slugify(source['title'], separator='_')
            if not update:
                yaml.dump({title: pipeline}, open(os.path.join(dirpath, PIPELINE_FILE), 'w'))
