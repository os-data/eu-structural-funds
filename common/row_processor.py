"""Boilerplate functionality for processors."""

import logging
import copy
import petl
import json
import itertools
import collections
import os
import sys

def processor():
    return "%-32s" % os.path.basename(sys.argv[0]).split('.')[0].title()

def _override_parameters(pipeline_parameters,
                         datapackage, resource_index,
                         processor_name=None):
    processor_name = processor_name or processor().lower().strip()
    parameters = copy.deepcopy(pipeline_parameters)

    datapackage_parameters = datapackage.get(processor_name, {})
    resource_properties = datapackage['resources'][resource_index]
    resource_parameters = resource_properties.get(processor_name, {})

    _check_parameters(pipeline_parameters,
                      datapackage_parameters,
                      resource_parameters)

    parameters.update(datapackage_parameters)
    parameters.update(resource_parameters)

    return parameters


def _check_parameters(*configuration):
    message = 'Parameters must be a dict, found {}'

    for parameters in configuration:
        if not isinstance(parameters, dict):
            raise TypeError(message.format(parameters))


def _write_to_log(parameter_view, sample_rows, resource_index):
    parameter_view = json.dumps(parameter_view, ensure_ascii=False, indent=4)
    table_view = petl.look(petl.fromdicts(sample_rows))

    logging.info('Processed resource %s', resource_index)
    logging.info('Parameters = %s', parameter_view)
    logging.info('Sample output: \n%s', table_view)


def _get_sample_rows(row_generator, sample_size):
    sample_rows = []
    for i, sample_row in enumerate(row_generator):
        sample_rows.append(sample_row)
        if i + 1 == sample_size:
            return sample_rows
    else:
        return sample_rows


Index = collections.namedtuple('Index', ['resource', 'row'])


# noinspection PyDefaultArgument
def process(resources,
            row_processor,
            pass_context=False,
            sample_size=15,
            verbose=True,
            parameters={},
            datapackage=None,
            nothing_to_report='_pass'):
    """Apply a row processor to each row of each datapackage resource.

    The function provides the following boilerplate functionality:

        * Override pipeline parameters with datapackage parameters
        * Record the parameters to the log
        * Force iteration over a sample of rows
        * Dump the data samples to the log
        * Collect statistics

    :param resources: a generator of generators of rows
    :param row_processor: a function that processes one row of data
    :param pass_context: whether to pass over the resource and row indices
    :param sample_size: the size of the data sample
    :param verbose: whether to log the parameters and the data sample
    :param parameters: the processing parameters found in `pipeline-spec.yaml`
    :param datapackage: required to override pipeline parameters
    :param nothing_to_report: tells the stats collector to skip this row

    :raises: `TypeError` if any of the parameters sets is not a `dict`

    :returns: the new_resource (a generator of generators)
    :returns: the processor report (a list of dicts)

    """

    processor_report = []

    def process_resources(parameters_):
        """Return a generator of resources."""

        for resource_index, resource in enumerate(resources):
            resource_report = {}

            if datapackage:
                parameters_ = _override_parameters(parameters_,
                                                   datapackage,
                                                   resource_index)

            def process_rows(resource_):
                """Return a generator of rows."""

                for row_index, row in enumerate(resource_):
                    index = Index(resource_index, row_index)
                    context = (index,) if pass_context else ()

                    row, row_report = row_processor(row, *context,
                                                    **parameters_)

                    if row_report != nothing_to_report:
                        resource_report.update({index.row: row_report})

                    yield row

            processor_report.append(resource_report)

            row_generator = process_rows(resource)
            sample_rows = list(_get_sample_rows(row_generator, sample_size))
            yield itertools.chain(sample_rows, row_generator)

            if verbose:
                _write_to_log(parameters_, sample_rows, resource_index)

    return process_resources(parameters), processor_report
