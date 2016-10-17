"""This module lists, validates and bootstraps sources.

    Context
    -------

    Contributors describe the data sources by filling up a pseudo-datapackage
    file named source.description.yaml and dropping it into the national or
    regional source folder. This (simplified) YAML format is chosen over the
    JSON datapackage format because its syntax is easier for non-programmers.

    Bootstrap process
    -----------------

    Unless a source folder is passed as an argument, the module recursively
    walks through the data directory and when its finds a folder that contains
    a source description file, bootstraps a pipeline specs file and a scraper
    placeholder if required. The module never overwrites existing files. The
    process will fail if the description file is ambiguous.

    Usages
    ------

    python3 -m common.bootstrap list [--validation]
    python3 -m common.bootstrap validate
    python3 -m common.bootstrap update
    python3 -m common.bootstrap update AT.austria/AT11.burgenland
    python3 -m common.bootstrap status [--pipeline|--description] AT.austria
    python3 -m common.bootstrap status [--table]

    This script only supports python3.

"""

import re
import os
import yaml
import json

from shutil import copyfile
from click import argument, command, group, secho, echo
from click import option
from petl import fromdicts, look, sort
from slugify import slugify
from jsonschema import Draft4Validator
from sqlalchemy.orm import sessionmaker
from yaml.parser import ParserError
from yaml.scanner import ScannerError
from os.path import join, exists, splitext
from datetime import datetime

from common.metrics import get_latest_stats
from common.metrics import Snapshot
from common.config import (
    PIPELINE_FILE,
    SOURCE_FILE,
    DATA_DIR,
    DEFAULT_PIPELINE_FILE,
    LOCAL_PATH_EXTRACTOR,
    REMOTE_EXCEL_EXTRACTOR,
    REMOTE_CSV_EXTRACTOR,
    TEMPLATE_SCRAPER_FILE,
    PROCESSORS_DIR,
    SCRAPER_FILE,
    DESCRIPTION_SCHEMA_FILE,
    DB_ENGINE
)

ERROR = dict(fg='red', bold=True)
WARN = dict(fg='yellow')
SUCCESS = dict(fg='blue')
COUNTRY = re.compile(r'/data/([A-Z]{2})\.')
NUTS = re.compile(r'([A-Z\d]{2,}\.[a-z]+)')


def collect_sources(pipeline_id=None):
    """Return a sorted list of Source objects."""

    now_ = datetime.now()
    session = sessionmaker(bind=DB_ENGINE)()

    if pipeline_id:
        return [Source(pipeline_id, timestamp=now_, db_session=session)]
    else:
        sources = []

        for folder, _, filenames in os.walk(DATA_DIR):
            if SOURCE_FILE in filenames:
                pipeline_id = folder.replace(DATA_DIR + '/', '')
                source = Source(pipeline_id,
                                timestamp=now_,
                                db_session=session)
                sources.append(source)
        return sorted(sources)


class Source(object):
    """This class represents a data source."""

    def __init__(self, pipeline_id, timestamp=None, db_session=None):
        self.timestamp = timestamp
        self.id = pipeline_id
        self.slug = slugify(self.id, separator='_')
        self.folder = join(DATA_DIR, *pipeline_id.split(os.sep))

        self.nuts_code = NUTS.findall(self.folder)[-1].split('.')[0]
        self.country_code = COUNTRY.search(self.folder).group(1)

        self.validation_status = 'broken'
        self.pipeline_status = 'down'
        self.traceback = None
        self.validation_errors = []

        self.description = self._read_description()
        self.pipeline = self._read_pipeline()
        self._validate()

        self.db_session = db_session

    def bootstrap(self):
        """Add a default pipeline file with the correct data extractor."""

        assert self.description, 'Description file not loaded'

        with open(join(self.folder, SOURCE_FILE)) as stream:
            self.description = yaml.load(stream)
        with open(DEFAULT_PIPELINE_FILE) as stream:
            default_pipeline = yaml.load(stream)

        self.pipeline[self.slug] = default_pipeline.pop('pipeline-id')
        extractor = self._get_extractor()

        if extractor:
            self.pipeline[self.slug]['pipeline'][1]['run'] = extractor

            with open(join(self.folder, PIPELINE_FILE), 'w') as stream:
                yaml.dump(self.pipeline, stream)

            message = '{}: added default {}'
            secho(message.format(self.id, PIPELINE_FILE), **SUCCESS)
            self.pipeline_status = 'up'

        else:
            message = '{}: ambiguous extractor'
            secho(message.format(self.id), **WARN)

    def save_scraper(self):
        """Copy the scraper placeholder to where it belongs."""

        if self.pipeline:
            scraper_folder = join(PROCESSORS_DIR, self.country_code)
            if not exists(scraper_folder):
                os.mkdir(scraper_folder)
            copyfile(TEMPLATE_SCRAPER_FILE, self.scraper_path)

            message = '{}: added default {}'
            secho(message.format(self.id, SCRAPER_FILE), **SUCCESS)

    @property
    def resource_type(self):
        """Return the resource type common to all resources else None."""

        values = set()

        if 'resources' in self.description:
            for resource in self.description['resources']:
                ambiguities = (
                    resource.get('path') and resource.get('url'),
                    not resource.get('path') and not resource.get('url')
                )

                if any(ambiguities):
                    return

                value = 'path' if resource.get('path') else 'url'
                values.add(value)

            if len(values) == 1:
                return values.pop()

    @property
    def extension(self):
        """Return the extension common to all resources else None."""

        if self.resource_type and not self.scraper_required:
            values = set()
            for resource in self.description['resources']:
                _, extension = splitext(resource[self.resource_type])
                values.add(extension)
            if len(values) == 1:
                value = values.pop()
                if '?' not in value:
                    return value

    @property
    def scraper_required(self):
        """Whether a pdf or web scraper is needed."""
        return self.description.get('scraper_required')

    @property
    def scraper_path(self):
        """Scrapers are named by slug and saved to common.processors.XX."""
        filename = self.slug + '_' + SCRAPER_FILE
        return join(PROCESSORS_DIR, self.country_code, filename)

    @property
    def color(self):
        """The ANSI color of the validation status."""
        return {
            'broken': ERROR,
            'loaded': WARN,
            'valid': SUCCESS
        }[self.validation_status]

    @property
    def has_scraper(self):
        """Whether the scraper module exists."""
        return exists(self.scraper_path)

    @property
    def updated(self):
        """Return the last bootstrap timestamp."""
        return (self.db_session.query(Snapshot, Snapshot.timestamp,
                                      Snapshot.pipeline_id == self.id)
                .order_by(Snapshot.timestamp.desc())
                .limit(1)
                .all()
                .pop()
                ).timestamp

    def dump_to_db(self):
        """Save the current state to the database."""

        update = Snapshot(
            pipeline_id=self.id,
            timestamp=self.timestamp,
            validation_status=self.validation_status,
            pipeline_status=self.pipeline_status,
            scraper_required=self.scraper_required,
            resource_type=self.resource_type,
            extension=self.extension,
            has_scraper=self.has_scraper,
            country_code=self.country_code,
            nuts_code=self.nuts_code,
            slug=self.slug,
            nb_validation_errors=len(self.validation_errors)
        )
        self.db_session.add(update)
        self.db_session.commit()

    def echo(self):
        """Echo the current state."""

        echo('pipeline_status = {}'.format(self.pipeline_status))
        echo('validation_status = {}'.format(self.validation_status))
        echo('nb_validation_errors = {}'.format(len(self.validation_errors)))
        echo('resource_type = {}'.format(self.resource_type))
        echo('extension = {}'.format(self.extension))
        echo('scraper_required = {}'.format(self.scraper_required))
        echo('has_scraper = {}'.format(self.has_scraper))

    def _read_description(self):
        try:
            with open(join(self.folder, SOURCE_FILE)) as stream:
                description = yaml.load(stream)
            self.validation_status = 'loaded'
            return description

        except (ParserError, ScannerError) as error:
            self.traceback = str(error)
            return {}

    def _read_pipeline(self):
        try:
            with open(join(self.folder, PIPELINE_FILE)) as stream:
                pipeline = yaml.load(stream)
                self.pipeline_status = 'up'
                return pipeline

        except FileNotFoundError:
            return {}

    def _validate(self):
        if self.description:
            with open(DESCRIPTION_SCHEMA_FILE) as stream:
                description_schema = yaml.load(stream)

            validator = Draft4Validator(description_schema)
            if validator.is_valid(self.description):
                self.validation_status = 'valid'

            errors = validator.iter_errors(self.description)
            self.validation_errors = sorted(errors, key=str)

    def _get_extractor(self):
        if self.scraper_required:
            return self._scraper_module()
        elif self.resource_type == 'path':
            return LOCAL_PATH_EXTRACTOR
        elif self.extension in ('xls', 'xlsx'):
            return REMOTE_EXCEL_EXTRACTOR
        elif self.resource_type == 'csv':
            return REMOTE_CSV_EXTRACTOR
        else:
            return

    def _scraper_module(self):
        module = 'processors.{}.{}-{}'
        name = SCRAPER_FILE.replace('.py', '')
        return module.format(self.id, self.country_code, self.slug, name)

    def __lt__(self, other):
        return self.id < other.id


@command('show')
@argument('pipeline_id', required=True)
@option('-d', '--description', is_flag=True, help='Show description file.')
def show_file(pipeline_id, description):
    """Show the pipeline (or description) file."""

    source = collect_sources(pipeline_id).pop()

    if description:
        echo(json.dumps(source.description, indent=4))
    else:
        echo(json.dumps(source.pipeline, indent=4))


@command('status')
@argument('pipeline_id', required=False)
@option('-t', '--table', is_flag=True, help='Verbose table format.')
def report_status(pipeline_id, table):
    """Show the current bootstrap status."""

    if pipeline_id:
        source = collect_sources(pipeline_id).pop()
        secho('Updated :{}'.format(source.updated), **SUCCESS)
        secho('Path: {}\n'.format(source.folder), **SUCCESS)
        source.echo()

    else:
        timestamp, stats, sums = get_latest_stats()
        secho('Last update: {}'.format(timestamp), **SUCCESS)
        secho('Total sources: {}\n'.format(stats.count()), **SUCCESS)

        if table:
            rows = [dict(zip(row.keys(), row)) for row in stats.all()]
            sorted_rows = sort(fromdicts(rows), key='pipeline_id')
            echo(look(sorted_rows, limit=None))
        else:
            for key, value in sums.items():
                args = key, value, value / stats.count()
                echo('{} = {} ({:.0%})'.format(*args))


@command('update')
@argument('pipeline_id', required=False)
def bootstrap_pipelines(pipeline_id=None):
    """Initialize pipelines where possible."""

    for source in collect_sources(pipeline_id=pipeline_id):
        if source.description:
            if not source.pipeline:
                source.bootstrap()
                if source.scraper_required:
                    if not source.has_scraper:
                        source.save_scraper()
            else:
                message = '{}: no changes made'
                echo(message.format(source.id))
        else:
            message = '{}: syntax error in description'
            secho(message.format(source.id), **ERROR)

        source.dump_to_db()


@command(name='list')
@option('-v', '--validation', is_flag=True, help='List by validation status.')
def list_pipelines(validation):
    """List sources by pipeline status."""

    sources = collect_sources()

    if validation:
        for source in sources:
            args = source.id, source.validation_status
            secho('{}: {}'.format(*args), **source.color)
    else:
        for source in sources:
            color = SUCCESS if source.pipeline else ERROR
            message = '{}: {}'.format(source.id, source.pipeline_status)
            secho(message, **color)

    message = '\nCollected {} description files'
    echo(message.format(len(sources)))


@command(name='validate')
def validate_descriptions():
    """List the bugs in the source descriptions.

    Description files are validated against a custom JSONSchema validation
    because the specs are somewhat different from a pure JSONTableSchema.
    The conversion happens later in the pipeline.
    """

    line = '-' * 80
    separator = '\n{}\n{:^80}\n'

    for source in collect_sources():
        secho(separator.format(line, source.id), **source.color)

        if source.validation_status == 'broken':
            message = '{}'.format(source.traceback)
        elif source.validation_status == 'loaded':
            errors = map(lambda e: e.message, source.validation_errors)
            message = '\n'.join(errors)
        else:
            message = 'Valid source description.'

        secho(message, **source.color)


@group()
def main():
    pass


main.add_command(list_pipelines)
main.add_command(bootstrap_pipelines)
main.add_command(validate_descriptions)
main.add_command(report_status)
main.add_command(show_file)


if __name__ == '__main__':
    main()
