import requests
import os
import json
import sys

CURRENCIES_FILE = os.path.join(os.path.dirname(__file__), 'processors', 'currencies.json')
CURRENCY_API = 'http://currencies.apps.grandtrunk.net/getrate/{1}-{2}-01/{0}/EUR'
currencies = json.load(open(CURRENCIES_FILE))

missing_keys = map(lambda x: x.strip(), sys.stdin.readlines())
for k in missing_keys:
    if k in currencies:
        continue
    tp = k.split('-')
    if int(tp[1]) >= 2017:
        tp = (tp[0], '2017', '01')
    if '-'.join(tp) in currencies:
        continue
    try:
        print("fetching %r" % (tp,))
        currencies[k] = requests.get(CURRENCY_API.format(*tp)).json()
    except:
        print("Couldn't fetch %r" % (tp,))

json.dump(currencies, open(CURRENCIES_FILE, 'w'))

