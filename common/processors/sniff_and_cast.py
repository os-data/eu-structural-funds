"""This processor parses and casts amounts and dates by sniffing data.

Assumptions
-----------

At this stage we assume that there is only one data resource.

Data processing
---------------

The processor goes through all fields in the schema and assigns them a caster.
If the field has a complete set of format parameters, a ready made caster is
used, else the processor sniffs the data to find the best match. If the
processor fails to sniff a caster, a `CasterNotFound` error is raised.

Dates require the `format` parameter. Numbers require the `groupChar` and
`decimalChar` parameters. Parameters can be set in the datapackage.

Datapackage mutation
--------------------

The processor toggles field types to match changes in the data.

"""

# TODO: relax the single resource constraint in `sniff_and_cast` processor

import petl

from copy import deepcopy
from math import ceil
from logging import warning, info
from datapackage_pipelines.wrapper import ingest, spew
from jsontableschema.exceptions import InvalidCastError
from jsontableschema.types import DateType, NumberType

from common.utilities import process, format_to_json, get_fiscal_fields
from common.config import (
    SNIFFER_SAMPLE_SIZE,
    SNIFFER_MAX_FAILURE_RATIO,
    DATE_FORMATS,
    NUMBER_FORMATS
)


class CasterNotFound(Exception):
    """A chatty exception class for when the sniffer fails."""

    template = (
        'Could not find a parser for {field}\n'
        'Tried\n{guesses} on {sample_size} rows\n'
        'Sample values =\n{sample_rows}\n'
        'Failed {nb_failures} times (maximum allowed = {max_nb_failures})'
    )

    def __init__(self, sniffer):
        self.sniffer = sniffer
        super(CasterNotFound, self).__init__(self._message)

    @property
    def _message(self):
        return self.template.format(
            field=self.sniffer.field['name'],
            guesses=format_to_json(self.sniffer.format_guesses),
            sample_rows=format_to_json(self.sniffer.sample_values),
            nb_failures=self.sniffer.nb_failures,
            max_nb_failures=self.sniffer.max_nb_failures,
            sample_size=self.sniffer.sample_size
        )


class BaseSniffer(object):
    """A class that tries very hard to find an appropriate caster."""

    jst_type_class = None
    format_keys = []
    format_guesses = []

    def __init__(self, field, resource_sample, max_failure_rate):
        self.max_failure_rate = max_failure_rate
        self.sample_values = self._get_field_sample(resource_sample, field)
        self.sample_size = len(self.sample_values)
        self.max_nb_failures = ceil(self.max_failure_rate * self.sample_size)

        # The following get updated with each guess
        self.format = {key: field.get(key) for key in self.format_keys}
        self.field = deepcopy(field)
        self.nb_failures = 0
        self._caster = None

    def get_caster(self):
        """Get a ready made caster if possible, else guess from the data."""

        if all(self.format.values()):
            self.field.update(self.format)
            return self.jst_type_class(self.field)
        else:
            return self._guess_caster()

    def cast(self, value):
        if self.jst_type_class == NumberType:
            return self._caster.cast_currency(value)
        return self._caster.cast(value)

    def _pre_cast_checks_ok(self, value):
        return True

    def _post_cast_check_ok(self, value):
        return True

    @staticmethod
    def _get_field_sample(resource_sample, field):
        """Return a subset of the relevant data column."""

        sample_table = petl.fromdicts(resource_sample)
        sample_column = list(petl.values(sample_table, field['name']))
        return sample_column

    def _guess_caster(self):
        """Return the first caster that succeeds."""

        for self.format in self.format_guesses:
            self.nb_failures = 0
            self.field.update(self.format)

            caster = self.jst_type_class(self.field)

            for i, raw_value in enumerate(self.sample_values):
                try:
                    assert self._pre_cast_checks_ok(raw_value)
                    casted_value = caster.cast(raw_value)
                    assert self._post_cast_check_ok(casted_value)
                except (AssertionError, InvalidCastError):
                    self.nb_failures += 1

            if self.nb_failures <= self.max_nb_failures:
                self._log_success()
                return caster

        raise CasterNotFound(self)

    def _log_success(self):
        message = (
            'Caster guess for %s is %s, '
            'number of failures = %s '
            '(allowed %s)'
        )
        args = (
            self.field['name'], self,
            self.nb_failures,
            self.max_nb_failures
        )
        info(message, *args)

    def __str__(self):
        return '{} {}'.format(self.__class__.__name__, self.format)

    def __repr__(self):
        return '<{}>'.format(self)


class DateSniffer(BaseSniffer):
    jst_type_class = DateType
    format_keys = ['format']
    format_guesses = DATE_FORMATS


class NumberSniffer(BaseSniffer):
    jst_type_class = NumberType
    format_keys = ['decimalChar', 'groupChar']
    format_guesses = NUMBER_FORMATS

    def _pre_cast_checks_ok(self, value):
        if value is not None:
            if value.count(self.format['decimalChar']) > 1:
                return False

            if self.format['decimalChar'] in value:
                decimal_index = value.find(self.format['decimalChar'])
                group_index = value.find(self.format['groupChar'])
                if decimal_index < group_index:
                    return False

        return True

    # noinspection PyMethodMayBeStatic
    def _post_cast_check_ok(self, value):
        if value is not None:
            value_as_string = str(value)
            if '.' in value_as_string:
                if len(value_as_string.split('.')[1]) > 2:
                    return False

        return True


def update_field_types(datapackage):
    """Update the field types by looking up the fiscal schema."""

    fields = datapackage['resources'][0]['schema']['fields']
    types_lookup = get_fiscal_fields('type')

    for field in fields:
        fiscal_type = types_lookup[field['name']]
        field.update(type=fiscal_type)

    return datapackage


def select_sniffer(field):
    """Select the sniffer according to the fiscal field type."""

    assert field['type'] in ('date', 'number')
    sniffer_name = field['type'].capitalize() + 'Sniffer'
    sniffer_class = globals()[sniffer_name]
    return sniffer_class


def get_casters(datapackage,
                resource_sample,
                max_failure_rate=SNIFFER_MAX_FAILURE_RATIO):
    """Return a caster for each fiscal field."""

    casters = {}

    for field in datapackage['resources'][0]['schema']['fields']:
        if field['type'] == 'string':
            caster = None

        else:
            sniffer_class = select_sniffer(field)
            sniffer = sniffer_class(field, resource_sample, max_failure_rate)
            caster = sniffer.get_caster()

        casters.update({field['name']: caster})

    return casters


def cast_values(row, casters, row_index=None):
    """Convert values to JSONTableSchema types."""

    for key, value in row.items():
        if casters.get(key):
            if value:
                try:
                    row[key] = casters[key].cast(value)
                except InvalidCastError:
                    message = 'Could not cast %s = %s'
                    warning(message, key, row[key])
                    row[key] = None

        else:
            if row_index == 0:
                message = (
                    '%s field is not in the datapackage, '
                    'data is out of sync with schema'
                )
                warning(message, key)

    return row


def extract_data_sample(resource, sample_size=SNIFFER_SAMPLE_SIZE):
    """Extract sample rows out of the resource."""

    data_sample = []

    for i, row in enumerate(resource):
        data_sample.append(row)
        if i + 1 == sample_size:
            break
    return data_sample, resource


def concatenate_data_sample(data_sample, resource):
    """Concatenate the sample rows back with the rest of the resource."""

    for row in data_sample:
        yield row
    for row in resource:
        yield row


if __name__ == '__main__':
    _, datapackage_, resources_ = ingest()
    resource_ = next(resources_)
    resource_sample_, resource_left_over_ = extract_data_sample(resource_)
    datapackage_ = update_field_types(datapackage_)
    casters_ = get_casters(datapackage_, resource_sample_)
    resource_ = concatenate_data_sample(resource_sample_, resource_left_over_)
    kwargs = dict(casters=casters_, pass_row_index=True)
    new_resources_ = process([resource_], cast_values, **kwargs)
    spew(datapackage_, new_resources_)
