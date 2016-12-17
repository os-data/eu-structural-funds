"""A processor to concatenate resources that have a common set of fields."""

from datapackage_pipelines.wrapper import ingest, spew


def concatenate(resources):
    """Concatenate multiple resources."""

    for resource in resources:
        for row in resource:
            yield row


if __name__ == '__main__':
    _, datapackage, resources_ = ingest()
    single_resource = concatenate(resources_)
    datapackage['resources'] = [datapackage['resources'][0]]
    spew(datapackage, [single_resource])
