"""This module defines the Milestone class."""


from glob import glob
from os.path import join, dirname, abspath
from datapackage.exceptions import ValidationError
from yaml import load
from slugify import slugify
from datapackage import DataPackage


ROOT_DIR = abspath(dirname(__file__))


def assemble(metadata_file):
    """Assemble a data-package from its descriptor parts."""

    def read(file):
        with open(file) as yaml:
            return load(yaml.read())

    def add_name(info):
        info['name'] = slugify(info['title'], separator='_')
        return info

    def get_files(filetype):
        filename = metadata_file.replace('metadata', filetype)
        folder = dirname(metadata_file)
        schema_files_pattern = join(folder, filename)
        return glob(schema_files_pattern)

    descriptor = add_name(read(metadata_file))
    resources = [add_name(read(file)) for file in get_files('resource')]
    model = get_files('model')

    descriptor['resources'] = resources
    if model and len(model) == 1:
        descriptor['model'] = model.pop()

    return DataPackage(descriptor)


class Milestone(DataPackage):
    def __init__(self, *args, **kwargs):
        super(Milestone, self).__init__(*args, **kwargs)

    def schema_errors(self):
        try:
            super(Milestone, self).validate()
        except ValidationError:
            for error in self.iter_errors():
                yield error.message


class MilestoneCollector(object):
    def __init__(self, root):
        self.root = root
        self.milestones = []

    def _collect(self):
        metadata_file_pattern = join(self.root, '*.metadata.yaml')
        for metadata_file in glob(metadata_file_pattern, recursive=True):
            milestone = assemble(metadata_file)
            self.milestones.append(milestone)

    @property
    def status(self):
        return {}