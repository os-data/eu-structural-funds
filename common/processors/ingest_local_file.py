"""An object oriented processor to ingest csv, json and excel files."""

# Design considerations
# ---------------------
#
# This processor is a replacement for the `stream_from_path` processor,
# whose functional approach became somewhat unreadable and brittle to changes.
# This new object oriented approach should improve readability and facilitate
# the handling of special cases through subclassing.

import json
import cchardet

from logging import warning, info, error

import logging
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.wrapper import spew
from os.path import splitext
from petl import fromdicts, look
from pip.utils import cached_property
from tabulator import Stream

from common.config import LOG_SAMPLE_SIZE
from common.utilities import format_to_json


class BaseIngestor(object):
    """A thin wrapper around the `tabulator.Stream` class."""

    VALID_EXTENSIONS = (
        '.csv',
        '.json',
        '.xls',
        '.xlsx'
    )

    @classmethod
    def load(cls, resource):
        """Return an instance of the relevant ingestor class."""

        filename = resource['path']
        _, extension = splitext(filename)
        assert extension in cls.VALID_EXTENSIONS

        ingestor_name = extension.lstrip('.').upper() + 'Ingestor'
        ingestor_class = globals()[ingestor_name]

        return ingestor_class(resource, extension)

    def __init__(self, resource, extension):
        self.resource = resource
        self.extension = extension

    @property
    def rows(self):
        """Return a generator of rows."""

        self._log_parameters()
        self._check_headers()

        info('Running preprocessors: %r', self._pre_processors)

        for pre_processor in self._pre_processors:
            pre_processor()

        info('Opening resource: %s', self.resource['path'])
        with Stream(self.resource['path'], **self._body_options) as stream:
            info('First %s rows =\n%s', LOG_SAMPLE_SIZE, self._show(stream))
            for row in stream.iter(keyed=True):
                yield row

    @property
    def _body_options(self):
        return {
            'headers': self._headers,
            'sample_size': LOG_SAMPLE_SIZE,
            'post_parse': self._post_processors,
        }

    @property
    def _header_options(self):
        return {'headers': int(self._parser_options.get('headerRow', 1))}

    @property
    def _parser_options(self):
        return self.resource.get('parser_options', {})

    def _check_headers(self):
        message = 'Fields and headers do no match'
        extra_fields = set(self._fields) - set(self._headers)
        extra_headers = set(self._headers) - set(self._fields)
        if len(extra_headers) > 0:
            logging.error('Headers in the source and not in the description file:')
            for header in extra_headers:
                logging.error('\t%s', header)
        if len(extra_fields) > 0:
            logging.error('Fields in the description file and not in the source:')
            for field in extra_fields:
                logging.error('\t%s', field)
        assert set(self._headers) == set(self._fields), message

    @property
    def _pre_processors(self):
        """A list of processors invoked before streaming."""
        return []

    @property
    def _post_processors(self):
        """A list of processors invoked after streaming."""
        return []

    @staticmethod
    def _lowercase_empty_values(rows):
        # This is a workaround waiting on the following to be fixed:
        # https://github.com/frictionlessdata/jsontableschema-py/issues/139
        for index, headers, row in rows:
            row = [field.lower() if field in ['None', 'Null', 'Nil', 'NaN']
                   else field for field in row]
            yield index, headers, row

    @staticmethod
    def force_strings(rows):
        """A post-parser processor to force all fields to strings."""

        for index, headers, values in rows:
            values_as_strings = \
                list(
                    map(
                        lambda x:
                        str(x).strip().replace('\n', ' ')
                        if x is not None
                        else '',
                        values
                    )
                )
            yield index, headers, values_as_strings

    @property
    def _raw_headers(self):
        """Headers as found in the data file."""

        with Stream(self.resource['path'], **self._header_options) as stream:
            return stream.headers

    @property
    def _headers(self):
        """Headers without redundant blanks and/or line breaks."""

        clean_headers = []

        for raw_header in self._raw_headers:
            if raw_header:
                tokens = raw_header.split()
                clean_field = ' '.join(tokens)
                clean_headers.append(clean_field)

        return clean_headers

    @property
    def _fields(self):
        """Fields expected in the data from the source file."""
        return [field['name'] for field in self.resource['schema']['fields']]

    def _log_parameters(self):
        """Record ingestion parameters to the log stream."""

        fields_as_json = format_to_json(sorted(self._fields))
        headers_as_json = format_to_json(sorted(self._headers))
        options_as_json = format_to_json(self._body_options)
        nb_empty_headers = len(self._fields) - len(self._headers)

        info('Ignoring %s empty header fields', nb_empty_headers)
        info('%s sourced fields = %s', len(self._fields), fields_as_json)
        info('%s data fields = %s', len(self._headers), headers_as_json)
        info('Ingestor options = %s', options_as_json)

    @staticmethod
    def _show(stream):
        """Return a table of sample data."""

        keyed_rows = []
        for row in stream.sample:
            keyed_rows.append(dict(zip(stream.headers, row)))
        petl_table = fromdicts(keyed_rows)

        return repr(look(petl_table, limit=None))

    def __repr__(self):
        args = self.__class__.__name__, self.resource['name']
        return '<{}: {}>'.format(*args)


class CSVIngestor(BaseIngestor):
    """An ingestor for csv files."""

    @property
    def _body_options(self):
        options = super(CSVIngestor, self)._body_options
        options.update(encoding=self._encoding)
        if self._parser_options.get('delimiter'):
            options.update(delimiter=self._parser_options['delimiter'])
        if self._parser_options.get('quotechar'):
            options.update(quotechar=self._parser_options['quotechar'])
        if self._parser_options.get('quoting'):
            options.update(quoting=self._parser_options['quoting'])
        return options

    @property
    def _post_processors(self):
        return [self._lowercase_empty_values,
                self._skip_header, self._drop_bad_rows, self.force_strings]

    @cached_property
    def _encoding(self):
        """Select or detect the file encoding and set the resource utf-8."""

        if self.resource.get('encoding'):
            return self.resource['encoding']
        else:
            return self._detect_encoding()

    def _detect_encoding(self):
        """Sniff the encoding using the entire file."""

        with open(self.resource['path'], 'rb') as stream:
            text = stream.read()
            encoding = cchardet.detect(text)['encoding']
            info('Detected %s encoding with cchardet', encoding)
            return encoding

    @property
    def _header_options(self):
        options = dict(headers=1, encoding=self._encoding)
        if self._parser_options.get('delimiter'):
            options.update(delimiter=self._parser_options['delimiter'])
        return options

    @staticmethod
    def _drop_bad_rows(rows):
        """Drop rows when they don't match headers (post-processor)."""

        for index, headers, row in rows:
            while len(row) > len(headers) and len(row[-1].strip()) == 0:
                row = row[:-1]
            if len(row) == len(headers):
                yield index, headers, row
            else:
                message = 'Bad row {}:\nheaders={}\nrow={}'\
                    .format(index, format_to_json(headers), format_to_json(row))
                assert False, message

    @staticmethod
    def _skip_header(rows):
        """Skip the header (post-processor)."""

        # Headers are passed as an option and need to be explicitly ignored.
        for index, headers, values in rows:
            if index != 1:
                yield index, headers, values


class JSONIngestor(BaseIngestor):
    """An ingestor for json files."""

    @property
    def _body_options(self):
        options = super(JSONIngestor, self)._body_options
        options.update(encoding=self._encoding)
        return options

    @cached_property
    def _encoding(self):
        """Select or detect the file encoding and set the resource utf-8."""

        if self.resource.get('encoding'):
            return self.resource['encoding']
        else:
            return 'utf-8'

    @property
    def _pre_processors(self):
        return [self._fill_missing_fields]

    @property
    def _post_processors(self):
        return [self._lowercase_empty_values,
                self.force_strings]

    @property
    def _fill_missing_fields(self):
        """Pre-fill incomplete JSON rows (to avoid fields mixing up)."""

        def fill_missing_fields():
            with open(self.resource['path']) as stream:
                rows = json.loads(stream.read())

            for row in rows:
                for header in self._raw_headers:
                    if header not in row:
                        row[header] = None

            with open(self.resource['path'], 'w+') as stream:
                stream.write(format_to_json(rows))

        return fill_missing_fields

    @cached_property
    def _raw_headers(self):
        """Return all field names encountered in the file."""

        keys = set()

        with open(self.resource['path']) as stream:
            rows = json.loads(stream.read())
            for row in rows:
                keys.update(set(row))

        return sorted(keys)


class XLSIngestor(BaseIngestor):
    """An ingestor for xls files."""

    @property
    def _post_processors(self):
        return [self._lowercase_empty_values,
                self._skip_header,
                self._fixed_points,
                self.force_strings]

    @staticmethod
    def _skip_header(rows):
        """Skip the header (post-processor)."""

        # Headers are passed as an option and need to be explicitly ignored.
        for index, headers, values in rows:
            if index != 1:
                yield index, headers, values

    @staticmethod
    def _fixed_points(rows):
        """Convert floats to 2-digit fixed precision strings"""

        for index, headers, values in rows:
            values = [
                '%.2f' % value if type(value) is float else value
                for value in values
            ]
            yield index, headers, values



XLSXIngestor = XLSIngestor


def ingest_resources(datapackage):
    """Ingest each resource one by one into the pipeline."""

    for resource in datapackage['resources']:
        ingestor = BaseIngestor.load(resource)
        yield ingestor.rows


if __name__ == '__main__':
    _, datapackage_, _ = ingest()
    resources = list(ingest_resources(datapackage_))
    spew(datapackage_, resources)
