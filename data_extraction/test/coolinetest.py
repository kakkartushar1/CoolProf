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

    def test_probabilities(self):
        col_census = MongoClient("localhost", 27017)["test_probs"]["test_census_surnames"]
        col_twitter = MongoClient("localhost", 27017)["test_probs"]["test_twitter_surnames"]
        result_col = MongoClient("localhost", 27017)["test_probs"]["test_result"]
        col_census.drop()
        col_twitter.drop()
        result_col.drop()
        surnames_census = [
            {"term": "garcia", "census_probability": 0.02},
            {"term": "fernandez", "census_probability": 0.01},
            {"term": "alvarez", "census_probability": 0.002}
        ]
        surnames_twitter = [
            {"term": "garcia", "count_users": 10},
            {"term": "fernandez", "count_users": 5},
            {"term": "alvarez", "count_users": 5}
        ]
        col_census.insert(surnames_census)
        col_twitter.insert(surnames_twitter)
        cooline.insert_terms_probabilities(dbname="test_probs", twitter_col_name="test_twitter_surnames",
                                           census_col_name="test_census_surnames", result_col_name="test_result")
        self.assertEqual(float(result_col.find_one({"term": "garcia"})["twitter_probability"]), 0.5)
        self.assertEqual(float(result_col.find_one({"term": "alvarez"})["twitter_probability"]), 0.25)
        self.assertEqual(float(result_col.find_one({"term": "fernandez"})["twitter_probability"]), 0.25)

        self.assertEqual(float(result_col.find_one({"term": "garcia"})["confidence"]), 0.04)
        self.assertEqual(float(result_col.find_one({"term": "alvarez"})["confidence"]), 0.008)
        self.assertEqual(float(result_col.find_one({"term": "fernandez"})["confidence"]), 0.04)

    def test_slugify_and_join_terms(self):
        terms = [
            {"_id": {"term": "garcia"}, "count_users": 5},
            {"_id": {"term": "garcía"}, "count_users": 10},
            {"_id": {"term": "Férnandez"}, "count_users": 5},
            {"_id": {"term": "fernandez"}, "count_users": 5}
        ]
        terms_col = MongoClient("localhost", 27017)["test"]["test_terms"]
        terms_col.drop()
        terms_col.insert(terms)
        result_col = MongoClient("localhost", 27017)["test"]["test_slugified"]
        result_col.drop()
        cooline.slugify_and_join_terms(dbname="test", top_terms_col_name="test_terms",
                                       result_col_name="test_slugified")
        self.assertEqual(float(result_col.find_one({"term": "fernandez"})["count_users"]), 10)
        self.assertEqual(float(result_col.find_one({"term": "garcia"})["count_users"]), 15)
        self.assertEqual(result_col.count(), 2)

    def test_extract_surnames(self):
        twitter_terms = [
            {"term": "garcia", "count_users": 5},
            {"term": "ruben", "count_users": 2}
            ]
        census_terms = [
            {"term": "garcia", "census_probability": 0.03}

        ]
        twitter_col = MongoClient("localhost", 27017)["test"]["test_twitter_terms"]
        twitter_col.drop()
        twitter_col.insert(twitter_terms)
        census_col = MongoClient("localhost", 27017)["test"]["test_census_terms"]
        census_col.drop()
        census_col.insert(census_terms)
        result_col = MongoClient("localhost", 27017)["test"]["test_result_extracted"]
        result_col.drop()
        cooline.extract_terms(dbname="test",
                              twitter_col_name="test_twitter_terms", census_col_name="test_census_terms",
                              result_col_name="test_result_extracted")
        self.assertEqual(float(result_col.find_one({"term": "garcia"})["count_users"]), 5)
        self.assertEqual(float(result_col.find_one({"term": "garcia"})["census_probability"]), 0.03)
        self.assertEqual(result_col.count(), 1)








