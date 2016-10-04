"""Pipeline configuration parameters."""

from os.path import dirname, abspath, join

OS_TYPES_URL = ('https://raw.githubusercontent.com/'
                'openspending/os-types/master/src/os-types.json')

PIPELINE_FILE = 'pipeline-spec.yaml'
DATAPACKAGE_FILE = 'source.datapackage.json'
DESCRIPTION_FILE = 'source.description.yaml'
EXTRACTOR_FILE = 'scraper.py'

ROOT_DIR = abspath(join(dirname(__file__), '..'))
PIPELINE_TEMPLATE = join(ROOT_DIR, 'plumbing', 'pipeline.template.yaml')
LOG_FILE = join(ROOT_DIR, 'plumbing', 'log', 'plumbing.log')
GEOCODES_FILE = join(ROOT_DIR, 'geography', 'geocodes.nuts.csv')
VALIDATOR_PROCESSOR = 'validate_datapackage'
DEFAULT_PIPELINE = {'schedule': {'crontab': '0 0 1 1 *'}, 'pipeline': []}

SPECIFICATIONS_DIR = join(ROOT_DIR, 'specifications')
CODELISTS_DIR = join(ROOT_DIR, 'codelists')

FISCAL_SCHEMA_FILE = join(SPECIFICATIONS_DIR, 'fiscal.schema.yaml')
FISCAL_MODEL_FILE = join(SPECIFICATIONS_DIR, 'fiscal.model.yaml')
FISCAL_DATAPACKAGE_FILE = join(SPECIFICATIONS_DIR, 'fiscal.datapackage.yaml')
