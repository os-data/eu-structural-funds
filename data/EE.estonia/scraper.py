import csv

import requests
from lxml import html
from petl import tojson, fromdicts

from common.config import JSON_FORMAT

BASE_URL = 'http://www.struktuurifondid.ee/list-of-beneficiaries/'


def scrape_measure(query, measure):
    if not len(measure):
        return
    query['meede'] = measure
    res = requests.get(BASE_URL, params=query)
    doc = html.fromstring(res.content)

    for row in doc.findall('.//table[@id="toetuste_saajad"]//tr'):
        data = {'measure': measure}
        cells = [c.text for c in row.findall('./td')]
        if len(cells) != 5:
            continue
        data['beneficiary'] = cells[0]
        data['name_of_project'] = cells[1]
        data['structural_assistance'] = cells[2]
        data['2nd_level_intermediate_body'] = cells[3]
        data['period'] = cells[4]

        from pprint import pprint
        pprint(data)
        yield data


def scrape():
    res = requests.get(BASE_URL)
    doc = html.fromstring(res.content)

    query = {}
    for inp in doc.findall('.//form[@name="toetuse_saajad"]//input'):
        if inp.get('type') == 'submit':
            continue
        query[inp.get('name')] = inp.get('value')

    rows = []
    for option in doc.findall('.//select[@id="meede"]/option'):
        measure = option.get('value')
        if len(measure):
            rows.extend(list(scrape_measure(query, measure)))

    tojson(fromdicts(rows), 'Estonia_scraper_dump.json',
           sort_keys=True, **JSON_FORMAT)


if __name__ == '__main__':
    scrape()
