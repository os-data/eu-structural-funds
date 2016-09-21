"""Pipeline configuration parameters."""

from os.path import dirname, abspath, join


DATAPACKAGE_FILE = 'source.description.json'
PIPELINE_SPECS = 'pipeline-specs.yaml'
DESCRIPTION_FILE = 'source.description.yaml'

ROOT_DIR = abspath(join(dirname(__file__), '..'))
PIPELINE_TEMPLATE = join(ROOT_DIR, 'plumbing', 'pipeline.template.yaml')
LOG_FILE = join(ROOT_DIR, 'plumbing', 'log', 'plumbing.log')
GEOCODES_FILE = join(ROOT_DIR, 'geography', 'geocodes.nuts.csv')
