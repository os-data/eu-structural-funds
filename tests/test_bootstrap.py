"""Unit-tests for the bootstrap module."""

import json
import os

from shutil import rmtree
from common.bootstrap import Source
from unittest import TestCase

from common.config import DATA_DIR, PIPELINE_FILE, SOURCE_FILE


class TestSource(TestCase):
    """Base class for testing the Source class."""

    pipeline_id = 'XX.dummy_id'

    pipeline_folder = os.path.join(DATA_DIR, pipeline_id)
    pipeline_source_file = os.path.join(pipeline_folder, SOURCE_FILE)
    pipeline_spec_file = os.path.join(pipeline_folder, PIPELINE_FILE)

    pipeline_spec = {
        'xx_dummy_id': {
            'schedule': 'dummy schedule',
            'pipeline': [
                {'run': 'processor 0'},
                {'run': 'processor 1'},
                {'run': 'processor 2'},
            ]
        }
    }

    source_description = {
        'name': 'dummy-name',
        'title': 'Dummy Name',
        'description': 'dummy description',
        'contributors': ['mickey mouse'],
        'scraper_required': False,
        'language': 'XX',
        'sources': [{
            'name': 'dummy source title',
            'web': 'http://dummy.org'
        }],
        'resources': [
            {
                'name': 'dummy resource 0',
                'title': 'Dummy Resource 0',
                'publication_date': "2010-1-1",
                'schema': {
                    'fields': [
                        {'name': 'dummy field 0'},
                        {'name': 'dummy field 1'},
                    ]
                }
            }
        ]
    }

    pipeline_after_remove = [
        {'run': 'processor 0'},
        {'run': 'processor 2'}
    ]

    pipeline_after_insert = [
        {'run': 'processor 0'},
        {'run': 'processor 1'},
        {'run': 'processor X'},
        {'run': 'processor 2'}
    ]

    def setUp(self):
        """Create a source object from temporary files."""

        # try:
        os.mkdir(self.pipeline_folder)
        # except FileExistsError:
        #     pass

        with open(self.pipeline_spec_file, 'w+') as stream:
            json.dump(self.pipeline_spec, stream)

        with open(self.pipeline_source_file, 'w+') as stream:
            json.dump(self.source_description, stream)

        self.source = Source(self.pipeline_id)

    def tearDown(self):
        rmtree(self.pipeline_folder)


class TestGeoProperties(TestSource):
    def setUp(self):
        """Create a source by passing the folder and test geo-properties."""

        try:
            os.mkdir(self.pipeline_folder)
        except FileExistsError:
            pass

        with open(self.pipeline_spec_file, 'w+') as stream:
            json.dump(self.pipeline_spec, stream)

        with open(self.pipeline_source_file, 'w+') as stream:
            json.dump(self.source_description, stream)

        self.source = Source(folder=self.pipeline_folder)

    def test_geographical_information(self):
        self.assertEquals(self.source.country_code, 'XX')
        self.assertEquals(self.source.country, 'NUTS LEVEL 1')
        self.assertEquals(self.source.nuts_code, 'XX')
        self.assertEquals(self.source.region, 'NUTS LEVEL 1')


class TestInitSource(TestSource):
    """Test instantiation."""

    def test_get_processor_index_returns_correct_index(self):
        self.assertEquals(self.source._get_processor_index('processor 0'), 0)

    def test_source_object_extracts_the_correct_country_and_nuts_codes(self):
        self.assertEquals(self.source.nuts_code, 'XX')
        self.assertEquals(self.source.country_code, 'XX')

    def test_source_is_loaded_and_pipeline_up(self):
        self.assertEquals(self.source.validation_status, 'valid')
        self.assertEquals(self.source.pipeline_status, 'up')
        self.assertEquals(self.source.traceback, None)
        self.assertEquals(self.source.validation_errors, [])


class TestModifyPipeline(TestSource):
    """Test inserting and removing pipeline processors."""

    def test_remove_processor_removes_the_correct_item(self):
        self.source.remove_processor('processor 1')
        self.assertListEqual(self.source._pipeline, self.pipeline_after_remove)

    def test_pass_bad_arguments_to_insert_processor_raises_assert_error(self):
        with self.assertRaises(AssertionError):
            self.source.insert_processor('X', index=1, after=True)
        with self.assertRaises(AssertionError):
            self.source.insert_processor('X', before=True, index=True)
        with self.assertRaises(AssertionError):
            self.source.insert_processor('X', before=True, after=True)
        with self.assertRaises(AssertionError):
            self.source.insert_processor('X', index=1, before=True, after=True)
        with self.assertRaises(AssertionError):
            self.source.insert_processor('X')

    def test_insert_processor_by_index(self):
        self.source.insert_processor('processor X', index=2)
        self.assertListEqual(self.source._pipeline, self.pipeline_after_insert)

    def test_insert_processor_after_another(self):
        self.source.insert_processor('processor X', after='processor 1')
        self.assertListEqual(self.source._pipeline, self.pipeline_after_insert)

    def test_insert_processor_before_another(self):
        self.source.insert_processor('processor X', before='processor 2')
        self.assertListEqual(self.source._pipeline, self.pipeline_after_insert)

    def test_insert_processor_with_parameters(self):
        self.source.insert_processor('processor X', before='processor 2',
                                     processor_parameters={'foo': 'bar'})
        self.assertEqual(self.source._pipeline[2]['parameters']['foo'], 'bar')
