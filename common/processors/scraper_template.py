"""The template for writing PDF and web scrapers."""

from datapackage_pipelines.wrapper import ingest, spew
from logging import debug


def scrape_beneficiaries(**params):
    """Return a generator of beneficiaries.

    Each beneficiary is a dictionary whose keys match the fields described
    in source.description.yaml. Parameters come from pipeline-specs.yaml.
    """

    debug('%s', **params)
    beneficiaries = [
        {'field1': 'foo', 'field2': 'spam'},
        {'field1': 'bar', 'field2': 'eggs'},
    ]
    for beneficiary in beneficiaries:
        yield beneficiary


if __name__ == '__main__':
    parameters, datapackage, _ = ingest()
    rows = scrape_beneficiaries(**parameters)
    spew(datapackage, [rows])
