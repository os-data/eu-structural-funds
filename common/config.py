"""Pipeline configuration parameters."""

from os.path import dirname, abspath, join
from sqlalchemy import create_engine

OS_TYPES_URL = ('https://raw.githubusercontent.com/'
                'openspending/os-types/master/src/os-types.json')

PIPELINE_FILE = 'pipeline-spec.yaml'
DATAPACKAGE_FILE = 'source.datapackage.json'
SOURCE_FILE = 'source.description.yaml'
STATUS_FILE = 'pipeline-status.json'
SCRAPER_FILE = 'scraper.py'

ROOT_DIR = abspath(join(dirname(__file__), '..'))
DATA_DIR = join(ROOT_DIR, 'data')
SPECIFICATIONS_DIR = join(ROOT_DIR, 'specifications')
PROCESSORS_DIR = join(ROOT_DIR, 'common', 'processors')
CODELISTS_DIR = join(ROOT_DIR, 'codelists')

GEOCODES_FILE = join(ROOT_DIR, 'geography', 'geocodes.nuts.csv')
FISCAL_SCHEMA_FILE = join(SPECIFICATIONS_DIR, 'fiscal.schema.yaml')
FISCAL_MODEL_FILE = join(SPECIFICATIONS_DIR, 'fiscal.model.yaml')
FISCAL_DATAPACKAGE_FILE = join(SPECIFICATIONS_DIR, 'fiscal.metadata.yaml')
DEFAULT_PIPELINE_FILE = join(SPECIFICATIONS_DIR, 'default-pipeline-spec.yaml')
TEMPLATE_SCRAPER_FILE = join(PROCESSORS_DIR, 'scraper_template.py')
DESCRIPTION_SCHEMA_FILE = join(SPECIFICATIONS_DIR, 'source.schema.json')
TEMPLATE_SOURCE_FILE = join(SPECIFICATIONS_DIR, SOURCE_FILE)

LOCAL_PATH_EXTRACTOR = 'stream_from_path'
REMOTE_CSV_EXTRACTOR = 'simple_remote_source'
REMOTE_EXCEL_EXTRACTOR = 'stream_remote_excel'

DB_URI = 'sqlite:///{}/metrics.sqlite'
DB_ENGINE = create_engine(DB_URI.format(ROOT_DIR))

DEFAULT_HEADER_LINES = 1
DEFAULT_ENCODING = 'utf-8'
DEFAULT_PARSER_OPTIONS = {'delimiter': ',', 'quotechar': '"'}
DEFAULT_VERBOSE = True
DEFAULT_SAMPLE_SIZE = 50
