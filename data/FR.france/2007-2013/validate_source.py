from datapackage_pipelines.wrapper import ingest, spew
import json
import os

print(os.getcwd())
datapackage_file, _, _ = ingest()
datapackage = json.loads(datapackage_file)

spew(datapackage, [])
