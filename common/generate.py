import os
import sys
import hashlib

import json
import yaml
import slugify

from .config import SOURCE_FILE, PIPELINE_FILE, DATA_DIR

PREPROCESSING = {
    'parse_currency_fields',
    'convert_excel_dates',
    'MT.parse_dates',
    'NL.parse_dates'
}

if __name__ == "__main__":

    update = False
    if len(sys.argv) > 1:
        if sys.argv[1] == 'update':
            update = True

    for dirpath, dirnames, filenames in os.walk(DATA_DIR):
        if SOURCE_FILE in filenames:
            source_raw = open(os.path.join(dirpath, SOURCE_FILE)).read().encode('utf8')
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

            pipeline = [
                ('read_description', {'save_datapackage': False,
                                      '_cache_buster': hashlib.md5(source_raw).hexdigest()}),
                ('ingest_local_file', {}),
                ('map_values', mappings),
                ('map_fields', {}),
                ('concatenate_identical_resources', {})
            ] + preprocessing + [
                ('sniff_and_cast', {}),
                ('reshape_data', {}),
                ('add_geocodes', {}),
                ('add_constants', {}),
                ('add_categories', {}),
                ('show_sample_in_console', {'sample_size': 1000}),
                ('load_fiscal_datapackage', {}),
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
