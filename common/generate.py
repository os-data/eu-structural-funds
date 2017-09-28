import os
import sys
import csv

import json

import logging
import yaml
import slugify

from .config import SOURCE_FILE, PIPELINE_FILE, DATA_DIR, FISCAL_SCHEMA_FILE, GEOCODES_FILE

PREPROCESSING = {
    'parse_currency_fields',
    'convert_excel_dates',
    'MT.parse_dates',
    'NL.parse_dates'
}

COUNTRY_CODES = [
    "",
    "IE",
    "LT",
    "ES",
    "MT",
    "BE",
    "IT",
    "AT",
    "LU",
    "SK",
    "EL",
    "PT",
    "PL",
    "FI",
    "NL",
    "EE",
    "DE",
    "CY",
    "HU",
    "SE",
    "UK",
    "DK",
    "FR",
    "SI",
    "LV",
    "BG",
    "CZ",
    "HR",
    "RO"
]

fiscal_schema = yaml.load(open(FISCAL_SCHEMA_FILE))
GEOCODES = list(csv.DictReader(open(GEOCODES_FILE)))


def _lookup_geocode(nuts_code):
    for info in GEOCODES:
        if info['NUTS-Code'] == nuts_code:
            return info['Description']

if __name__ == "__main__":

    update = False
    if len(sys.argv) > 1:
        if sys.argv[1] == 'update':
            update = True

    deps = {}

    for dirpath, dirnames, filenames in os.walk(DATA_DIR):
        if SOURCE_FILE in filenames:
            try:
                source = yaml.load(open(os.path.join(dirpath, SOURCE_FILE)))
                source['name'] = slugify.slugify(source['title'], separator='-').lower()
                relpath = dirpath.split('/data/')[1].split('/')
                prefix = '-'.join(x.lower() for x in relpath)
                geo = {
                    'country_code': relpath[0].split('.')[0],
                }
                geo.update({
                    'nuts_code': \
                        relpath[1].split('.')[0] \
                        if len(relpath)>1 and '.' in relpath[1] \
                        else geo['country_code']
                })
                geo.update({
                    'country': _lookup_geocode(geo['country_code']),
                    'region': _lookup_geocode(geo['nuts_code'])
                })
                source['geo'] = geo
                if update:
                    try:
                        current = yaml.load(open(os.path.join(dirpath, PIPELINE_FILE)))
                    except:
                        continue
                    current = list(current.values())[0]['pipeline']

                sources = []
                for datasrc in source.get('sources', []):
                    if 'name' in datasrc:
                        sources.append({
                            'title': datasrc['name'],
                            'url': datasrc.get('web', '')
                        })
                if len(sources) > 0:
                    source['sources'] = sources

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

                for resource in source['resources']:
                    schema = resource.get('schema', {})
                    if schema is not None:
                        for field in schema.get('fields', []):
                            tokens = field['name'].split()
                            clean_field = ' '.join(tokens)
                            field['name'] = clean_field

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
                                if field['name'] not in aliases and field['name'] != maps_to:
                                    aliases.append(field['name'])
                                del field['maps_to']
                                if 'translates_to' in field:
                                    del field['translates_to']
                concat_parameters = {'fields': concat_parameters}
                sniff_and_cast_parameters = {}
                if source.get('currency_symbol') is not None:
                    sniff_and_cast_parameters['currency_symbol'] = source['currency_symbol']
                currency_conversion = []
                if source.get('currency_code', 'EUR') != 'EUR':
                    currency_conversion.append(
                        ('currency_convert', {
                            'column': 'amount',
                            'currency-column': 'currency',
                            'currency': source.get('currency_code', 'EUR'),
                            'date-columns': [
                                'publication_date',
                                'starting_date',
                                'completion_date',
                                'final_payment_date'
                            ]
                        })
                    )

                threshold = 40
                pipeline = [
                    ('read_description', {'datapackage': source}),
                    ('ingest_local_file', {}),
                    ('map_values', mappings),
                    # ('map_fields', {}),
                    # ('concatenate_identical_resources', {})
                    ('add_constants', {}),
                    ('concatenate', concat_parameters),
                    ('add_row_id', {'prefix': prefix}),
                ] + preprocessing + [
                    ('reshape_data', {}),
                    ('show_sample_in_console', {'sample_size': 20}),
                    ('add_geocodes', geo),
                    ('add_categories', {}),
                    ('handle_amounts', {
                        'column-order': [
                            'eu_cofinancing_amount',
                            'eu_cofinancing_amount_eligible',
                            'total_amount',
                            'total_amount_eligible',
                         ],
                        'target-column': 'amount',
                        'kind-column': 'amount_kind',
                    }),
                    ('fingerprint_beneficiaries', {}),
                    ('fiscal.model', fiscal_model_parameters),
                    ('sniff_and_cast', sniff_and_cast_parameters),
                ] + currency_conversion + [
                    ('validate_values', {
                        'thresholds': {
                            'beneficiary_id': threshold,
                            'funding_period': threshold,
                            'amount': threshold,
                            'amount_kind': threshold,
                            'beneficiary_country_code': threshold,
                            'beneficiary_nuts_code': threshold,
                            'fund_acronym': threshold,
                            'beneficiary_country': threshold,
                            'beneficiary_nuts_region': threshold,
                        },
                        'allowed_values': {
                            'fund_acronym': ['ERDF', 'ESF', 'CF', 'other'],
                            'funding_period': ['2000-2006', '2007-2013', '2014-2020']
                        }
                    }),
                    ('dump.to_zip', {'out-file': 'fiscal.datapackage.zip'}),
                    ('fiscal.upload', {'in-file': 'fiscal.datapackage.zip', 'publish': True}),
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
                    'title': source['title'],
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
                deps.setdefault(geo['country_code'], []).append(os.path.join('./eu-structural-funds/data', *relpath, title))
                deps.setdefault('', []).append(os.path.join('./eu-structural-funds/data', *relpath, title))
            except:
                logging.exception('Problem in %s', dirpath)
                raise

    concats = {}
    for country in COUNTRY_CODES:
        pipeline_id = 'eu-esif-funds-full'
        suffix = ''
        prefix = ''
        title_suffix = ''
        if len(country) > 0:
            suffix = '-' + country.lower()
            title_suffix = ' (Filtered by %s)' % country.upper()
            prefix = country.lower() + '-'
        concats[pipeline_id + suffix] = {
            'title': 'Complete European ESIF Funds Beneficiaries 2007-2020' + title_suffix,
            'dependencies': [
                {'pipeline': x} for x in deps[country]
            ],
            'pipeline':
                [
                    {
                        'run': 'collect_all_sources',
                        'parameters': {
                            'country': country
                        }
                    },
                    {
                        'run': 'stream_remote_resources'
                    },
                    {
                        'run': 'concatenate',
                        'parameters': {
                            'target': {
                                'name': prefix + 'eu-esif-funds-beneficiaries-2007-2020-full'
                            },
                            'fields': {
                                "amount": [],
                                "amount_kind": [],
                                "approval_date": [],
                                "beneficiary_address": [],
                                "beneficiary_city": [],
                                "beneficiary_country": [],
                                "beneficiary_country_code": [],
                                "beneficiary_county": [],
                                "beneficiary_id": [],
                                "beneficiary_name": [],
                                "beneficiary_nuts_code": [],
                                "beneficiary_nuts_region": [],
                                "beneficiary_person": [],
                                "beneficiary_postal_code": [],
                                "beneficiary_url": [],
                                "cci_program_code": [],
                                "completion_date": [],
                                "currency": [],
                                "eu_cofinancing_amount": [],
                                "eu_cofinancing_amount_eligible": [],
                                "eu_cofinancing_rate": [],
                                "final_payment_date": [],
                                "first_payment_date": [],
                                "fund_acronym": [],
                                "funding_period": [],
                                "management_authority": [],
                                "member_state_amount": [],
                                "operational_programme": [],
                                "priority_label": [],
                                "priority_number": [],
                                "project_description": [],
                                "project_name": [],
                                "project_status": [],
                                "source": [],
                                "starting_date": [],
                                "theme_code": [],
                                "theme_name": [],
                                "third_party_amount": [],
                                "total_amount": [],
                                "total_amount_applied": [],
                                "total_amount_eligible": [],
                                "internal_id": []
                            }
                        }
                    },
                    {
                        'run': 'add_metadata',
                        'parameters': {
                            'name': prefix + 'eu-esif-funds-beneficiaries-2000-2020-full',
                            'title': 'Complete European ESIF Funds Beneficiaries 2007-2020' + title_suffix,
                            'description': "Structural and Cohesion Funds are financial tools set up to implement "
                                           "the regional policy of the European Union. They aim to reduce regional "
                                           "disparities in income wealth and opportunities. The overall budget "
                                           "for the 2007-2013 period was €347 billion according to Wikipedia. This "
                                           "repository is a data pipeline. It channels information about the "
                                           "beneficiaries of the funds into the OpenSpending data store. "
                                           "The goal is to provide a unified data-set that is easy to visualize and "
                                           "query so that citizens and journalists can follow the money on a local "
                                           "and global scale. This project is a collaborative effort between "
                                           "Open-Knowledge Germany, Open Knowledge International and a number of "
                                           "journalists and developers.",
                            'sources': [
                                {
                                    'title': 'EU ESIF Portal',
                                    'url':
                                        'https://www.fi-compass.eu/esif/european-structural-and-investment-funds-esif'
                                },
                                {
                                    'title': 'Inforegio EU Regional Policy Portal',
                                    'url': 'http://ec.europa.eu/regional_policy/en/'
                                },
                            ],
                            'author': 'Adam Kariv <adam.kariv@okfn.org>',
                            'contributors': [
                                "Loic Jounot <loic@cyberpunk.bike>",
                                "Bela Seeger <bela.seeger@okfn.de>",
                                "Anna Alberts <anna.alberts@okfn.de>",
                            ],
                            'fiscalPeriod': {
                                'start': '2000-01-01',
                                'stop': '2020-12-31'
                            },
                            'regionCode': 'eu'
                        }
                    },
                    {
                        'run': 'fiscal.model',
                        'parameters': {
                            'options': {
                                'eu_cofinancing_amount': {'currency': 'EUR'},
                                'eu_cofinancing_amount_eligible': {'currency': 'EUR'},
                                'member_state_amount': {'currency': 'EUR'},
                                'third_party_amount': {'currency': 'EUR'},
                                'total_amount': {'currency': 'EUR'},
                                'total_amount_applied': {'currency': 'EUR'},
                                'total_amount_eligible': {'currency': 'EUR'},
                                'amount': {'currency': 'EUR'},
                            },
                            'os-types': {
                                'amount': 'value',
                                'amount_kind': 'value-kind:code',
                                'approval_date': 'date:fiscal:activity-approval',
                                'beneficiary_address': 'geo-source:address:street-address:description',
                                'beneficiary_city': 'geo-source:address:city:code',
                                'beneficiary_country': 'geo-source:address:country:label',
                                'beneficiary_country_code': 'geo-source:address:country:code',
                                'beneficiary_county': 'geo-source:address:county:code',
                                'beneficiary_id': 'recipient:generic:id',
                                'beneficiary_name': 'recipient:generic:name',
                                'beneficiary_nuts_code': 'geo-source:address:region:code',
                                'beneficiary_nuts_region': 'geo-source:address:region:label',
                                'beneficiary_person': 'recipient:generic:legal-entity:point-of-contact:description',
                                'beneficiary_postal_code': 'geo-source:address:zip:code',
                                'beneficiary_url': 'recipient:generic:url',
                                'cci_program_code': 'administrative-classification:generic:level3:code:full',
                                'completion_date': 'date:fiscal:activity-end',
                                'currency': 'unknown:string',
                                'eu_cofinancing_amount': 'value',
                                'eu_cofinancing_amount_eligible': 'value',
                                'eu_cofinancing_rate': 'unknown:string',
                                'final_payment_date': 'date:fiscal:final-payment',
                                'first_payment_date': 'date:fiscal:first-payment',
                                'fund_acronym': 'fin-source:generic:level2:code:full',
                                'funding_period': 'fin-source:generic:level1:code',
                                'management_authority': 'fin-source:generic:level3:code:full',
                                'member_state_amount': 'value',
                                'operational_programme': 'administrative-classification:generic:level4:code:full',
                                'priority_label': 'administrative-classification:generic:level2:label',
                                'priority_number': 'administrative-classification:generic:level2:code:full',
                                'project_description': 'recipient:generic:legal-entity:receiving-project:description',
                                'project_name': 'recipient:generic:legal-entity:code',
                                'project_status': 'recipient:generic:legal-entity:receiving-project:status',
                                'source': 'unknown:string',
                                'starting_date': 'date:fiscal:activity-start',
                                'theme_code': 'administrative-classification:generic:level1:code',
                                'theme_name': 'administrative-classification:generic:level1:label',
                                'third_party_amount': 'value',
                                'total_amount': 'value',
                                'total_amount_applied': 'value',
                                'total_amount_eligible': 'value',
                                'internal_id': 'transaction-id:code'
                            },
                            'titles': {
                                'amount': 'Cost of the project',
                                'amount_kind': 'Kind of Amount',
                                'approval_date': 'Approval date of the project',
                                'beneficiary_address': 'Address',
                                'beneficiary_city': 'City',
                                'beneficiary_country': 'Country',
                                'beneficiary_country_code': 'Country code',
                                'beneficiary_county': 'County',
                                'beneficiary_id': 'Beneficiary ID',
                                'beneficiary_name': 'Beneficiary name',
                                'beneficiary_nuts_code': 'NUTS code',
                                'beneficiary_nuts_region': 'NUTS region',
                                'beneficiary_person': 'Beneficiary person',
                                'beneficiary_postal_code': 'Postal code',
                                'beneficiary_url': 'Beneficiary website',
                                'cci_program_code': 'CCI code',
                                'completion_date': 'Completion date of the project',
                                'currency': 'Original currency code',
                                'eu_cofinancing_amount': 'EU co-financing',
                                'eu_cofinancing_amount_eligible': 'eligible EU co-financing',
                                'eu_cofinancing_rate': 'EU co-financing rate',
                                'final_payment_date': 'Date of the final payment',
                                'first_payment_date': 'Date of the first payment',
                                'fund_acronym': 'Fund',
                                'funding_period': 'Funding period',
                                'management_authority': 'Management Authority',
                                'member_state_amount': 'Amount of national and regional funding',
                                'operational_programme': 'Operational Programme',
                                'priority_label': 'Priority',
                                'priority_number': 'Priority number',
                                'project_description': 'Project description',
                                'project_name': 'Project name',
                                'project_status': 'Project status',
                                'source': 'Source URL',
                                'starting_date': 'Starting date of the project',
                                'theme_code': 'Thematic objective code',
                                'theme_name': 'Thematic objective',
                                'third_party_amount': 'Third party funding',
                                'total_amount': 'Total cost of the project',
                                'total_amount_applied': 'Total amount the project applied for',
                                'total_amount_eligible': 'Total eligible expenditure',
                                'internal_id': 'Automatically assigned row id'
                            }
                        }
                    },
                    {
                        'run': 'dump.to_zip',
                        'parameters': {
                            'out-file': prefix+'fiscal.datapackage.zip'
                        }
                    },
                    {
                        'run': 'fiscal.upload',
                        'parameters': {
                            'in-file': prefix+'fiscal.datapackage.zip',
                            'publish': True
                        }
                    }
                ]
        }
    yaml.dump(concats, open(os.path.join(DATA_DIR, 'pipeline-spec.yaml'), 'w'))

