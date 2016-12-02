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

from logging import warning, info
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

        for pre_processor in self._pre_processors:
            pre_processor()

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
        return {'headers': 1}

    def _check_headers(self):
        message = 'Fields and headers do no match'
        assert set(self._headers) == set(self._fields), message

    @property
    def _pre_processors(self):
        """A list of processors invoked before streaming."""
        return []

    @property
    def _post_processors(self):
        """A list of processors invoked after streaming."""
        return []

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
        return options

    @property
    def _post_processors(self):
        return [self._skip_header, self._drop_bad_rows]

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
        return dict(headers=1, encoding=self._encoding)

    @staticmethod
    def _drop_bad_rows(rows):
        """Drop rows when they don't match headers (post-processor)."""

        for index, headers, row in rows:
            if len(row) == len(headers):
                yield index, headers, row
            else:
                warning('Bad row %s = %s', index, format_to_json(row))

    @staticmethod
    def _skip_header(rows):
        """Skip the header (post-processor)."""

        # Headers are passed as an option and need to be explicitly ignored.
        for index, headers, row in rows:
            if index != 1:
                yield index, headers, row


class JSONIngestor(BaseIngestor):
    """An ingestor for json files."""

    @property
    def _pre_processors(self):
        return [self._fill_missing_fields]

    @property
    def _fill_missing_fields(self):
        """Pre-fill incomplete JSON rows (to avoid fields mixing up)."""

        def fill_missing_fields():
            with open(self.resource['path']) as stream:
                rows = json.loads(stream.read())

            for row in rows:
                for header in self._headers:
                    if header not in row:
                        row[header] = None

            with open(self.resource['path'], 'w+') as stream:
                stream.write(format_to_json(rows))

        return fill_missing_fields

    @property
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

    @staticmethod
    def force_strings(rows):
        """A post-parser processor to force all fields to strings."""

        for index, headers, values in rows:
            values_as_strings = list(map(str, values))
            yield index, headers, values_as_strings

    @property
    def _post_processors(self):
        return [self._skip_header, self.force_strings]

    @staticmethod
    def _skip_header(rows):
        """Skip the header (post-processor)."""

        # Headers are passed as an option and need to be explicitly ignored.
        for index, headers, row in rows:
            if index != 1:
                yield index, headers, row


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
