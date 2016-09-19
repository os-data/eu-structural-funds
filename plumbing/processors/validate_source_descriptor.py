"""A pipeline processor to validate the source datapackage."""

from datapackage import DataPackage
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.wrapper import spew
from jsonschema import ValidationError
from datetime import datetime

from yaml import dump

now = str(datetime.now())


def validate_schema():
    """Validate a data-package schema generated from a description file."""

    params, _, _ = ingest()

    with open('datapackage.source.json') as json:
        descriptor = json.read()

    package = DataPackage(descriptor)
    report = dict(timestamp=now)

    try:
        package.validate()
        report.update(
            is_valid=True,
        )

    except ValidationError:
        errors = []
        for error in package.iter_errors():
            errors.append(error.message)

        report.update(
            is_valid=False,
            errors=errors
        )

    with open('datapackage.source.report.yaml', 'w+') as json:
        json.write(dump(report))

    spew(package, None)


if __name__ == "__main__":
    validate_schema()
