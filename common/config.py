"""Pipeline configuration parameters."""

from os.path import dirname, abspath, join
from sqlalchemy import create_engine

OS_TYPES_URL = ('https://raw.githubusercontent.com/'
                'openspending/os-types/master/src/os-types.json')

PIPELINE_FILE = 'pipeline-spec.yaml'
SOURCE_DATAPACKAGE_FILE = 'source.datapackage.json'
SOURCE_FILE = 'source.description.yaml'
STATUS_FILE = 'pipeline-status.json'
SCRAPER_FILE = 'scraper.py'
SOURCE_ZIP = 'source.datapackage.zip'
FISCAL_ZIP_FILE = 'fiscal.datapackage.zip'
SOURCE_DB = 'source.db.xlsx'
DATAPACKAGE_FILE = 'datapackage.json'

ROOT_DIR = abspath(join(dirname(__file__), '..'))
DATA_DIR = join(ROOT_DIR, 'data')
SPECIFICATIONS_DIR = join(ROOT_DIR, 'specifications')
PROCESSORS_DIR = join(ROOT_DIR, 'common', 'processors')
CODELISTS_DIR = join(ROOT_DIR, 'codelists')
DROPBOX_DIR = join(ROOT_DIR, 'dropbox')

GEOCODES_FILE = join(ROOT_DIR, 'geography', 'geocodes.nuts.csv')
FISCAL_SCHEMA_FILE = join(SPECIFICATIONS_DIR, 'fiscal.schema.yaml')
FISCAL_MODEL_FILE = join(SPECIFICATIONS_DIR, 'fiscal.model.yaml')
FISCAL_METADATA_FILE = join(SPECIFICATIONS_DIR, 'fiscal.metadata.yaml')
DEFAULT_PIPELINE_FILE = join(SPECIFICATIONS_DIR, 'default-pipeline-spec.yaml')
TEMPLATE_SCRAPER_FILE = join(PROCESSORS_DIR, 'scraper_template.py')
DESCRIPTION_SCHEMA_FILE = join(SPECIFICATIONS_DIR, 'source.schema.json')
TEMPLATE_SOURCE_FILE = join(SPECIFICATIONS_DIR, SOURCE_FILE)

LOCAL_PATH_EXTRACTOR = 'ingest_local_file'
REMOTE_CSV_EXTRACTOR = 'simple_remote_source'
REMOTE_EXCEL_EXTRACTOR = 'stream_remote_excel'
DATAPACKAGE_MUTATOR = 'mutate_datapackage'

DB_URI = 'sqlite:///{}/metrics.sqlite'
DB_ENGINE = create_engine(DB_URI.format(ROOT_DIR))

VERBOSE = False
LOG_SAMPLE_SIZE = 15
JSON_FORMAT = dict(indent=4, ensure_ascii=False, default=repr)
SNIFFER_SAMPLE_SIZE = 5000
SNIFFER_MAX_FAILURE_RATIO = 0.01
IGNORED_FIELD_TAG = '_ignored'
UNKNOWN_FIELD_TAG = '_unknown'
WARNING_CUTOFF = 10

NUMBER_FORMATS = [
    {'format': 'default', 'bareNumber': False, 'decimalChar': '.', 'groupChar': ','},
    {'format': 'default', 'bareNumber': False, 'decimalChar': ',', 'groupChar': '.'},
    {'format': 'default', 'bareNumber': False, 'decimalChar': '.', 'groupChar': ' '},
    {'format': 'default', 'bareNumber': False, 'decimalChar': ',', 'groupChar': ' '},
    {'format': 'default', 'bareNumber': False, 'decimalChar': '.', 'groupChar': ''},
    {'format': 'default', 'bareNumber': False, 'decimalChar': '.', 'groupChar': '`'},
    {'format': 'default', 'bareNumber': False, 'decimalChar': ',', 'groupChar': '\''},
    {'format': 'default', 'bareNumber': False, 'decimalChar': ',', 'groupChar': ' '},
]

DATE_FORMATS = [
    {'format': '%Y'},
    {'format': '%d/%m/%Y'},
    {'format': '%d//%m/%Y'},
    {'format': '%d-%b-%Y'},  # abbreviated month
    {'format': '%d-%b-%y'},  # abbreviated month
    {'format': '%d. %b %y'},  # abbreviated month
    {'format': '%b %y'},  # abbreviated month
    {'format': '%d/%m/%y'},
    {'format': '%d-%m-%Y'},
    {'format': '%Y-%m-%d'},
    {'format': '%y-%m-%d'},
    {'format': '%y.%m.%d'},
    {'format': '%Y.%m.%d'},
    {'format': '%d.%m.%Y'},
    {'format': '%d.%m.%y'},
    {'format': '%d.%m.%Y %H:%M'},
    {'format': '%Y-%m-%d %H:%M:%S'},
    {'format': '%Y-%m-%d %H:%M:%S.%f'},
    {'format': '%Y-%m-%dT%H:%M:%SZ'},
    {'format': '%m/%d/%Y'},
    {'format': '%m/%Y'},
    {'format': '%y'},
]
