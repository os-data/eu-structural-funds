"""Plumbing configuration."""

from os.path import dirname, abspath, join
from logging import info


ROOT_DIR = abspath(join(dirname(__file__), '..'))
PIPELINE_TEMPLATE = join(ROOT_DIR, 'plumbing', 'pipeline.template.yaml')
DATAPACKAGE_FILE = 'source.description.json'
PIPELINE_SPECS = 'pipeline-specs.yaml'
GEOCODE_COLUMN = 4
LOG_FILE = join(ROOT_DIR, 'plumbing', 'log', 'plumbing.log')


# info('Root folder: %s', ROOT_DIR)
