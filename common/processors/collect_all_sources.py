import os
import logging
import gobble
import requests
from slugify import slugify

from datapackage_pipelines.wrapper import ingest, spew

resources = []
datapackage = {
    'name': 'placeholder',
    'resources': resources
}

userid = gobble.user.User().id
for p, _, _ in os.walk('.'):
    dataset_name = slugify(p).lower()
    url_base = 'http://datastore.openspending.org/{}/{}'.format(userid, dataset_name)
    resp = requests.get(url_base+'/datapackage.json')
    if resp.status_code == 200:
        datapackage_json = resp.json()
        resource = datapackage_json['resources'][0]
        resource_url = '{}/{}'.format(url_base, resource['path'])
        resources.append({
            'url': resource_url,
            'name': dataset_name,
            'encoding': 'utf-8'
        })
        logging.error(resource_url)

parameters, _, _ = ingest()
spew(datapackage, [])
