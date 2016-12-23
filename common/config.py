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

VERBOSE = True
LOG_SAMPLE_SIZE = 15
JSON_FORMAT = dict(indent=4, ensure_ascii=False, default=repr)
SNIFFER_SAMPLE_SIZE = 5000
SNIFFER_MAX_FAILURE_RATIO = 0.05
IGNORED_FIELD_TAG = '_ignored'
UNKNOWN_FIELD_TAG = '_unknown'
WARNING_CUTOFF = 10

NUMBER_FORMATS = [
    {'format': 'currency', 'decimalChar': '.', 'groupChar': ','},
    {'format': 'currency', 'decimalChar': ',', 'groupChar': '.'},
    {'format': 'currency', 'decimalChar': '.', 'groupChar': ' '},
    {'format': 'currency', 'decimalChar': ',', 'groupChar': ' '},
    {'format': 'currency', 'decimalChar': '.', 'groupChar': ''},
    {'format': 'currency', 'decimalChar': '.', 'groupChar': '`'},
    {'format': 'currency', 'decimalChar': ',', 'groupChar': '\''},
    {'format': 'currency', 'decimalChar': ',', 'groupChar': ' '},
]

DATE_FORMATS = [
    {'format': 'fmt:%y'},
    {'format': 'fmt:%Y'},
    {'format': 'fmt:%d/%m/%Y'},
    {'format': 'fmt:%m/%d/%Y'},
    {'format': 'fmt:%d-%b-%Y'},  # abbreviated month
    {'format': 'fmt:%y-%m-%d'},
    {'format': 'fmt:%Y-%m-%d'},
    {'format': 'fmt:%y.%m.%d'},
    {'format': 'fmt:%d.%m.%Y'},
    {'format': 'fmt:%d.%m.%y'},
    {'format': 'fmt:%Y-%m-%d 00:00:00'},
    {'format': 'fmt:%Y-%m-%d 00:00:00.000'},
    {'format': 'fmt:%Y-%m-%d 00:00:00.000000'},
    {'format': 'fmt:%Y-%m-%d 01:22:49'},
]
