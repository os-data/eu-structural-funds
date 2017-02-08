import os
import logging
import gobble
import requests

import yaml
from slugify import slugify

from datapackage_pipelines.wrapper import ingest, spew

FILENAME = 'pipeline-spec.yaml'

resources = []
datapackage = {
    'name': 'placeholder',
    'resources': resources
}

parameters, _, _ = ingest()
country = parameters.get('country').lower()

userid = gobble.user.User().id
for dirpath, dirnames, filenames in os.walk('.'):
    if dirpath == '.':
        continue
    if FILENAME in filenames:
        pipeline = yaml.load(open(os.path.join(dirpath, FILENAME)))
        dataset_name = pipeline[list(pipeline.keys())[0]]['pipeline'][0]['parameters']['datapackage']['name']
        url_base = 'http://datastore.openspending.org/{}/{}'.format(userid, dataset_name)
        resp = requests.get(url_base+'/datapackage.json')
        if resp.status_code == 200:
            datapackage_json = resp.json()
            if len(country) > 0:
                if datapackage_json.get('geo', {}).get('country_code', 'xx').lower() != country:
                    continue
            resource = datapackage_json['resources'][0]
            resource_url = '{}/{}'.format(url_base, resource['path'])
            resources.append({
                'url': resource_url,
                'name': dataset_name,
                'encoding': 'utf-8',
                'delimiter': ',',
                'doublequote': True,
                'quoteChar': '"',
                'skipinitialspace': False
            })
            logging.error(resource_url)

spew(datapackage, [])
