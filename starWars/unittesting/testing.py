from etl_pipeline.extract import *
from etl_pipeline.transform import *
import unittest
import pymongo
import json
import requests

client = pymongo.MongoClient()
db = client["starwars"]


class TestEtlPipeline(unittest.TestCase):
    extract = Extract()
    transform = Transform()

    def test_retrieve_starship_info(self):
        assert(bool(self.extract.starships_data) is True)

    def test_retrieve_starships(self):
        assert(bool(self.extract.starship_names_list) is True)

    def test_retrieve_pilot_urls(self):
        assert(bool(self.extract.pilot_urls_nested_list) is True)

    def test_retrieve_pilot_names(self):
        assert (bool(self.extract.pilot_names_nested_list) is True)

    def test_retrieve_pilot_object_ids(self):
        assert(bool(self.extract.retrieve_pilot_object_ids()[0]) is True)
        assert (bool(self.extract.retrieve_pilot_object_ids()[1]) is True)

    def test_create_pilot_id_dict(self):
        assert(type(self.transform.create_pilot_id_dict()) == dict)
        assert (type(self.transform.create_pilot_id_dict()) is True)

    def test_starship_names_dict(self):
        assert(type(self.transform.starship_names_dict) == dict)
        assert(type(self.transform.starship_names_dict) is True)

    def test_create_starship_ids_dict(self):
        assert(type(self.transform.create_starship_ids_dict()) == dict)
        assert(type(self.transform.create_starship_ids_dict()) is True)

    def test_create_starships_collection(self):
        assert 'starships' in list(db.collection_names(include_system_collections=False))

    def test_insert_pilots_into_starships(self):
        for name in self.transform.pilot_id_dict.keys():
            assert name in (list(db.starships.find({'pilot': 1})))


