from datapackage_pipelines.wrapper import ingest, spew
import json
import os
import logging

import csv

_, dp, iter_res = ingest()

rows = []

for res in iter_res:
    for i, row in enumerate(res):
        rows.append(row)
        logging.debug(row)
        if i > 20:
            break

spew(dp, iter_res)

from datapackage import datapackage
