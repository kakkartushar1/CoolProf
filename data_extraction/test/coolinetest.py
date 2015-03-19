__author__ = 'rubcuevas'

import unittest
import lib.cooline as cooline
from pymongo import MongoClient


class MyTestCase(unittest.TestCase):


    def test_compound_insertion(self):
        col = MongoClient("localhost", 27017)["test_names"]["test_compound"]
        col.drop()
        cooline.insert_compound_names_as_simple("files/compounded_names.xlsx", "test_names", "test_compound", population=100)
        self.assertEqual(float(col.find_one({"term": "jose"})["census_probability"]), 0.2)
        self.assertEqual(float(col.find_one({"term": "francisco"})["census_probability"]), 0.3)
        self.assertEqual(float(col.find_one({"term": "ruben"})["census_probability"]), 0.1)
        self.assertEqual(float(col.find_one({"term": "javier"})["census_probability"]), 0.2)
        self.assertTrue(col.count(), 4)



if __name__ == '__main__':
    unittest.main()
