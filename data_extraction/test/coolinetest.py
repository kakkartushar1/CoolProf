__author__ = 'rubcuevas'

import unittest
import lib.cooline as cooline
from pymongo import MongoClient


class MyTestCase(unittest.TestCase):

    def test_compound_insertion(self):
        col = MongoClient("localhost", 27017)["test_names"]["test_compound"]
        col.drop()
        cooline.insert_names_census_freqs("files/compounded_names.xlsx", "test_names", "test_compound", population=100)
        self.assertEqual(float(col.find_one({"term": "jose"})["census_probability"]), 0.2)
        self.assertEqual(float(col.find_one({"term": "francisco"})["census_probability"]), 0.3)
        self.assertEqual(float(col.find_one({"term": "ruben"})["census_probability"]), 0.1)
        self.assertEqual(float(col.find_one({"term": "javier"})["census_probability"]), 0.2)
        self.assertTrue(col.count(), 4)
        col.drop()

    def test_surnames(self):
        col = MongoClient("localhost", 27017)["test_surnames"]["test_surnames"]
        col.drop()
        cooline.insert_surnames_census_freqs(
            freqs_path="files/surnames_test.xlsx", dbname="test_surnames", census_col_name="test_surnames", population=100)
        self.assertEqual(float(col.find_one({"term": "garcia"})["census_probability"]), 0.15)
        self.assertEqual(float(col.find_one({"term": "alvarez"})["census_probability"]), 0.32)
        self.assertEqual(float(col.find_one({"term": "cascos"})["census_probability"]), 0.08)
        self.assertEqual(float(col.find_one({"term": "fernandez"})["census_probability"]), 0.02)
        col.drop()



