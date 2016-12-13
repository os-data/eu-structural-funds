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

    Usage
    -----

    This module supports python3. For help: python3 -m common.bootstrap.

"""

import re
import os
import yaml

# noinspection PyPackageRequirements
from click import BadParameter
from pandas import DataFrame
from collections import Counter
from copy import deepcopy
from shutil import copyfile
from jsonschema import FormatChecker
from petl import fromdicts, look, sort, cut
from slugify import slugify
from jsonschema import Draft4Validator
from sqlalchemy.orm import sessionmaker
from yaml.parser import ParserError
from yaml.scanner import ScannerError
from os.path import join, exists, splitext
from datetime import datetime
from click import (command, secho, echo, option,
                   pass_context, group, Choice, argument)

from common.metrics import Snapshot
from common.utilities import get_fiscal_field_names, processor_names, GEOCODES
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
    DB_ENGINE,
    DATAPACKAGE_MUTATOR,
    DROPBOX_DIR,
    SOURCE_ZIP,
    ROOT_DIR, SOURCE_DB, FISCAL_ZIP_FILE
)


ERROR = dict(fg='red', bold=True)
WARN = dict(fg='yellow')
SUCCESS = dict(fg='blue')
COUNTRY = re.compile(r'/data/([A-Z]{2})\.')
NUTS = re.compile(r'([A-Z\d]{2,}\.[a-z]+)')


class Source(object):
    """This class represents a data source."""

    db_keys = Snapshot.__table__.columns.keys()

    def __init__(self,
                 pipeline_id=None,
                 folder=None,
                 timestamp=None,
                 db_session=None,
                 data_dir=DATA_DIR):

        message = 'Source takes either folder or pipeline_id argument'
        assert bool(pipeline_id) != bool(folder), message

        if pipeline_id:
            self.id = pipeline_id
            self.folder = join(data_dir, *pipeline_id.split(os.sep))

        if folder:
            self.folder = folder
            self.id = os.sep.join(folder.split(os.sep)[1:])

        self.timestamp = timestamp
        self.slug = slugify(self.id, separator='_')
        self.pipeline_spec_file = join(data_dir, PIPELINE_FILE)
        self.description_file = join(data_dir, SOURCE_FILE)

        self.nuts_code = NUTS.findall(self.folder)[-1].split('.')[0]
        self.country_code = COUNTRY.search(self.folder).group(1)
        self.country = self._lookup_geocode(self.country_code)
        self.region = self._lookup_geocode(self.nuts_code)

        self.validation_status = 'broken'
        self.pipeline_status = 'down'
        self.traceback = None
        self.validation_errors = []

        self.description = self._read_description()
        self.pipeline_spec = self._read_pipeline_spec()
        self._validate()

        self.db_session = db_session

    def bootstrap(self):
        """Add a default pipeline file with the correct data extractor."""

        assert self.description, 'Description file not loaded'

        with open(join(self.folder, SOURCE_FILE)) as stream:
            self.description = yaml.load(stream)
        with open(DEFAULT_PIPELINE_FILE) as stream:
            default_pipeline = yaml.load(stream)

        self.pipeline_spec[self.slug] = default_pipeline.pop('pipeline-id')
        extractor = self._get_extractor()

        if extractor:
            self._pipeline[1]['run'] = extractor
            if self.extension in ('.json', '.xls', '.xlsx'):
                self._pipeline.insert(
                    2, {'run': DATAPACKAGE_MUTATOR}
                )

            if len(self.description['resources']) > 1:
                for i, processor in enumerate(
                        self._pipeline):
                    print(processor)
                    if processor['run'] == 'reshape_data':
                        self._pipeline.insert(
                            i + 1, {'run': 'concatenate_identical_resources'}
                        )
                        break

            with open(join(self.folder, PIPELINE_FILE), 'w') as stream:
                yaml.dump(self.pipeline_spec, stream)

            message = '{}: added default {}'
            secho(message.format(self.id, PIPELINE_FILE), **SUCCESS)
            self.pipeline_status = 'up'

        else:
            message = '{}: ambiguous extractor'
            secho(message.format(self.id), **WARN)

    def save_scraper(self):
        """Copy the scraper placeholder to where it belongs."""

        if self.pipeline_spec:
            scraper_folder = join(PROCESSORS_DIR, self.country_code)
            if not exists(scraper_folder):
                os.mkdir(scraper_folder)
            copyfile(TEMPLATE_SCRAPER_FILE, self.scraper_path)

            message = '{}: added default {}'
            secho(message.format(self.id, SCRAPER_FILE), **SUCCESS)

    @property
    def _pipeline(self):
        return self.pipeline_spec[self.slug]['pipeline']

    @property
    def resource_type(self):
        """Return the resource type common to all resources else None."""

        values = set()

        if 'resources' in self.description:
            for resource in self.description['resources']:
                if not resource:
                    return

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
    def fiscal_zip_file(self):
        """Return the fiscal datapackage."""

        filepath = join(self.folder, FISCAL_ZIP_FILE)
        if exists(filepath):
            return filepath

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
    def has_scraper(self):
        """Whether the scraper module exists."""
        return exists(self.scraper_path)

    @property
    def updated_on(self):
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

        row = deepcopy(dict(self.state))
        del row['id']
        row.update(pipeline_id=self.id)
        self.db_session.add(Snapshot(**row))
        self.db_session.commit()

    @property
    def nb_validation_errors(self):
        return len(self.validation_errors)

    @property
    def state(self):
        for key in self.db_keys:
            if key == 'pipeline_id':
                yield key, self.id
            else:
                yield key, getattr(self, key)

    @property
    def flat_fields(self):
        """Return a flat representation of the source (one field per line)."""

        mappings = []

        for resource in self.description['resources']:
            if 'schema' in resource and 'fields' in resource['schema']:
                for field in resource['schema']['fields']:
                    mapping = {}

                    mapping.update(**dict(self.state))
                    mapping.update(**field)

                    for key, value in self.description.items():
                        if key != 'resources':
                            mapping.update({key: value})

                    for key, value in resource.items():
                        if key != 'schema':
                            mapping.update({key: value})
                        if key == 'title':
                            mapping.update(resource_title=value)

                    mappings.append(mapping)

        return mappings

    @property
    def fields_mapping(self):
        mappings = []
        for resource in self.description['resources']:
            if 'schema' in resource and 'fields' in resource['schema']:
                for field in resource['schema']['fields']:
                    mapping = {'pipeline_id': self.id}
                    mapping.update(**field)
                    mappings.append(mapping)
        return mappings

    def remove_processor(self, name):
        """Remove a processor by name."""
        index = self._get_processor_index(name)
        del self._pipeline[index]

    def insert_processor(self, name,
                         before=None, after=None, index=None,
                         processor_parameters=None):
        """Insert a processor in the pipeline."""

        positions = list(map(bool, [before, after, index]))
        assert positions.count(True) == 1

        processor = {'run': name}
        if processor_parameters:
            processor.update(parameters=processor_parameters)

        if before:
            index = self._get_processor_index(before)
        if after:
            index = self._get_processor_index(after) + 1

        self._pipeline.insert(index, processor)

    def save_pipeline_spec(self):
        with open(join(self.folder, PIPELINE_FILE), 'w+') as stream:
            yaml.dump(self.pipeline_spec, stream,
                      default_flow_style=False,
                      allow_unicode=True)

    def save_description(self):
        with open(join(self.folder, SOURCE_FILE), 'w+') as stream:
            yaml.dump(self.description, stream,
                      default_flow_style=False,
                      allow_unicode=True)

    @property
    def processors(self):
        return [processor['run'] for processor in self._pipeline]

    def _get_processor_index(self, name):
        """Return the index of a pipeline"""

        for i, processor in enumerate(self._pipeline):
            if processor['run'] == name:
                return i

        raise ValueError('{} processor not in pipeline'.format(name))

    @property
    def _pipeline(self):
        return self.pipeline_spec[self.slug]['pipeline']

    def _read_description(self):
        try:
            with open(join(self.folder, SOURCE_FILE)) as stream:
                description = yaml.load(stream)
            self.validation_status = 'loaded'
            return description

        except (ParserError, ScannerError) as error:
            self.traceback = str(error)
            return {}

    def _read_pipeline_spec(self):
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

            validator = Draft4Validator(
                description_schema,
                format_checker=FormatChecker()
            )
            if validator.is_valid(self.description):
                self.validation_status = 'valid'

            errors = validator.iter_errors(self.description)
            messages = [error.message for error in errors]
            self.validation_errors = sorted(messages)

    def _get_extractor(self):
        if self.scraper_required:
            return self._scraper_module()

        if self.resource_type == 'url':
            if self.extension == '.csv':
                return REMOTE_CSV_EXTRACTOR
            elif self.extension in ('.xls', 'xlsx'):
                return REMOTE_EXCEL_EXTRACTOR
            else:
                return
        elif self.resource_type == 'path':
            return LOCAL_PATH_EXTRACTOR
        else:
            return

    def _scraper_module(self):
        module = 'processors.{}.{}-{}'
        name = SCRAPER_FILE.replace('.py', '')
        return module.format(self.id, self.country_code, self.slug, name)

    @staticmethod
    def _lookup_geocode(nuts_code):
        for info in GEOCODES:
            if info['NUTS-Code'] == nuts_code:
                return info['Description']

    def __lt__(self, other):
        return self.id < other.id

    def __str__(self):
        return self.id

    def __repr__(self):
        return '<Source: {}>'.format(self)


@command('db')
@pass_context
def dump_database(ctx):
    """Dump a flat database of all the sources, one field per line.."""

    mappings = DataFrame()

    for source in ctx.obj['sources']:
        mappings = mappings.append(source.flat_fields, ignore_index=True)

    filepath = join(ROOT_DIR, SOURCE_DB)
    mappings.to_excel(filepath, sheet_name='All sourced fields', index=False)
    secho('Dumped the source database to {}'.format(filepath), **SUCCESS)


@command('status')
@pass_context
def report_pipeline_status(ctx):
    """Report the current status of the pipeline(s)."""

    for source in ctx.obj['sources']:
        secho('\nID: {}'.format(source.id), **SUCCESS)
        secho('Updated :{}'.format(source.updated_on), **SUCCESS)
        for contributor in source.description.get('contributors', []):
            secho('Author: {}'.format(contributor), **SUCCESS)

        echo()
        for key in source.db_keys:
            if key not in ('pipeline_id', 'id', 'timestamp'):
                echo('{} = {}'.format(key, getattr(source, key)))


@command('stats')
@pass_context
def compute_stats(ctx):
    """Compute bootstrap current stats."""

    rows = [dict(source.state) for source in ctx.obj['sources']]
    message = '\nNumber of pipelines = {}\n'
    secho(message.format(len(rows)), **SUCCESS)

    for key in Source.db_keys:
        if key not in ('timestamp', 'id', 'pipeline_id'):
            if key == 'nb_validation_errors':
                stat = sum([row[key] for row in rows])
            elif key in ('nuts_code', 'country_code'):
                stat = len({row[key] for row in rows})
            else:
                stat = dict(Counter([row[key] for row in rows]))
            echo('{}: {}'.format(key, stat))


@command('table')
@pass_context
def print_table(ctx):
    """Output a list of pipelines as table."""

    rows = [dict(source.state) for source in ctx.obj['sources']]
    message = '\nNumber of pipelines = {}\n'
    secho(message.format(len(rows)), **SUCCESS)

    subset = [
        'id',
        'pipeline_status',
        'validation_status',
        'nb_validation_errors',
        'scraper_required',
        'resource_type',
        'extension'
    ]
    sorted_rows = sort(cut(fromdicts(rows), *subset), key='id')
    echo(look(sorted_rows, limit=None))


@command('collect')
@pass_context
def copy_zip_files(ctx):
    """Copy all zipped source packages to one folder."""

    for source in ctx.obj['sources']:
        target_path = join(DROPBOX_DIR, source.slug + '.zip')
        source_path = join(source.folder, SOURCE_ZIP)
        args = source.id, source_path

        if exists(source_path):
            copyfile(source_path, target_path)
            secho('{}: copied {}'.format(*args), **SUCCESS)
        else:
            secho('{}: no {} to copy'.format(*args), **ERROR)

    echo('\nDestination folder: {}'.format(DROPBOX_DIR))


@command('update')
@pass_context
def bootstrap_pipelines(ctx):
    """Bootstrap pipelines where possible."""

    for source in ctx.obj['sources']:
        if source.description:
            if not source.pipeline_spec:
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
@option('--validation', is_flag=True, help='List by validation status.')
@pass_context
def list_pipelines(ctx, validation):
    """List sources by pipeline status."""

    level = {
        True: {'broken': ERROR, 'loaded': WARN, 'valid': SUCCESS},
        False: {'up': SUCCESS, 'down': ERROR}
    }

    sources = ctx.obj['sources']
    for source in sources:
        if validation:
            status = source.validation_status
        else:
            status = source.pipeline_status
        color = level[bool(validation)][status]
        secho('{}: {}'.format(source.id, status), **color)

    message = '\nCollected {} description files'
    echo(message.format(len(sources)))


@command(name='validate')
@pass_context
def validate_descriptions(ctx):
    """Validate source description files."""

    level = {'broken': ERROR, 'loaded': WARN, 'valid': SUCCESS}
    valid_keys = str(get_fiscal_field_names()).replace('[', '[None, ')

    for source in ctx.obj['sources']:
        color = level[source.validation_status]
        echo('\n{}\n'.format(join('data', source.id, SOURCE_FILE)))
        messages = []

        if source.validation_status == 'broken':
            messages.append('{}'.format(source.traceback))

        elif source.validation_status == 'loaded':
            for e in source.validation_errors:
                error = e.replace(valid_keys, 'fiscal fields')
                messages.append(error)
        else:
            messages.append('Valid :-)')

        message = '{}'.format('\n'.join(messages))
        secho(message, **color)


@command(name='pipeline')
@argument('action', type=Choice(('insert', 'remove')), required=True)
@argument('processor', type=Choice(processor_names), required=True)
@option('--before', type=Choice(processor_names))
@option('--after', type=Choice(processor_names))
@option('--parameter', type=(str, str), nargs=2, multiple=True)
@pass_context
def modify_pipeline(ctx, action, processor, before, after, parameter):
    """Insert or delete pipeline processors."""

    for source in ctx.obj['sources']:
        if not source.pipeline_spec:
            message = '{}: no pipeline found, skipping'
            secho(message.format(source.id), **ERROR)
            continue

        if action == 'remove':
            if processor in source.processors:
                source.remove_processor(processor)
            else:
                message = '{}: {} not in pipeline, skipping'
                secho(message.format(source.id, processor, **WARN))

        if action == 'insert':
            if before and after:
                raise BadParameter('Ambiguous position')

            position = before or after

            if position in source.processors:
                if processor in source.processors:
                    message = '{}: {} already exists'
                    secho(message.format(source.id, processor, **WARN))

                positions = dict(before=before, after=after)
                positions.update(processor_parameters=dict(parameter))
                source.insert_processor(processor, **positions)

            else:
                message = '{}: skipped because {} not in pipeline'
                secho(message.format(source.id, position), **WARN)

        source.save_pipeline_spec()
        message = '{}: saved changes'
        secho(message.format(source.id), **SUCCESS)


def collect_sources(select=None, **kwargs):
    """Return a sorted list of sources."""

    sources = []
    for folder, _, filenames in os.walk(DATA_DIR):
        if SOURCE_FILE in filenames:
            subfolder = folder.replace(DATA_DIR + '/', '')
            source = Source(subfolder, **kwargs)
            sources.append(source)

    if not select:
        return sorted(sources)

    subset = set()
    for key, value in select.items():
        for source in sources:
            if getattr(source, key) is value or getattr(source, key) == value:
                subset.add(source)

    return sorted(subset)


@group()
@option(
    '--select', type=(str, str), multiple=True,
    help='KEY VALUE selector (Source class attributes).'
)
@pass_context
def main(ctx, select):
    """Bootstrap command tools."""

    select = dict(select)
    for key, value in select.items():
        select[key] = None if value == 'None' else value

    ctx.obj['sources'] = collect_sources(
        select=select,
        timestamp=datetime.now(),
        db_session=sessionmaker(bind=DB_ENGINE)()
    )


main.add_command(list_pipelines)
main.add_command(bootstrap_pipelines)
main.add_command(validate_descriptions)
main.add_command(report_pipeline_status)
main.add_command(compute_stats)
main.add_command(print_table)
main.add_command(copy_zip_files)
main.add_command(dump_database)
main.add_command(modify_pipeline)


if __name__ == '__main__':
    main(obj={})
