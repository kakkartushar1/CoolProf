from lib import coolfunctions as cf

__author__ = 'rubcuevas'
from pymongo import MongoClient
import unittest
import pandas as pd

HOST = "localhost"
PORT = 27017
DBNAME = "test_cooldb"
COLNAME = "test_cooltweets"
TOKENS_FREQ_FILE = "/Users/rubcuevas/coolprof/tokens_freq.csv"
NUM_MIN_USERS = 2
tweets = [{"user": {"name": "Rubén-Cuevas Ruben9",
                    "screen_name": "rubcuevas", "description": "Soy español y me gusta el deporte"}},
                    {"user": {"name": "Rubén      Cuevas",
                              "screen_name": "rubcuevas", "description": ""}},
                    {"user": {"name": "Rubén",
                              "screen_name": "rubcuevas", "description": ""}},
                    {"user": {"name": " Rubén\u2318", "screen_name": "rcuevas33",
                              "description": "Español, I \u2665 fiesta y deporte"}},
                    {"user": {"name": "Pepe ",
                              "screen_name": "kiki", "description": "Soy adicto al deporte"}},
                    {"user": {"name": " Pepe ",
                              "screen_name": "lulu", "description": "Padre orgulloso"}},
                    {"user": {"name": "Ruben", "screen_name": "rubcuevas",
                              "description": ""}},
                    {"user": {"name": "Ruben",
                              "screen_name": "rubengarcia", "description": "Esposo y padre"}},
                    {"user": {"name": "¿¿¿Ruben???...Ruben..",
                              "screen_name": "garciaruben", "description": "Soy informático y tengo un hijo. Puxa Asturies!!!."}},
                    {"user": {"name": "Ruben",
                              "screen_name": "tututu", "description": ""}},
                    {"user": {"name": "\u2318 ¿!//Ruben??!", "screen_name": "messi",
                              "description": ""}},
                    {"user": {"name": "Mario \u2318", "screen_name": "mmario",
                              "description": ""}},
                    {"user": {"name": "Jeje \u2318", "screen_name": "00mario00",
                              "description": ""}},
                    {"user": {"name": "Mario", "screen_name": "mmario",
                              "description": ""}},
                    {"user": {"name": "D4niel", "screen_name": "dani1",
                              "description": ""}},
                    {"user": {"name": "D4niel", "screen_name": "dani2",
                              "description": ""}}
          ]

class TestCoolfunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        col = MongoClient("localhost", 27017)[DBNAME][COLNAME]
        col.insert(tweets)

    @classmethod
    def tearDownClass(cls):
        db = MongoClient("localhost", 27017)[DBNAME]
        db.drop_collection(COLNAME)


    def test_top_full_name_terms(self):
        tokens_freq = cf.top_full_name_terms(HOST, PORT, DBNAME, COLNAME, NUM_MIN_USERS)
        solution = {
            "rubén": 4,
            "pepe": 2,
            "ruben": 6

        }
        self.assertEquals(solution, tokens_freq.to_dict())

    def test_top_bio_terms(self):
        tokens_freq = cf.top_bio_terms(HOST, PORT, DBNAME, COLNAME, NUM_MIN_USERS)
        solution = {
            "deporte": 3,
            "padre": 2,
            "español": 2

        }
        self.assertEquals(solution, tokens_freq.to_dict())


    def test_write_tokens_freq(self):
        cf.top_terms_to_csv(cf.top_full_name_terms(
           HOST, PORT, DBNAME, COLNAME, NUM_MIN_USERS
        ), TOKENS_FREQ_FILE)
        freqs = pd.read_csv(TOKENS_FREQ_FILE, sep=";")
        self.assertEquals(freqs[freqs["token"] == "ruben"].iloc[0]["frequency"], 6)
        self.assertEquals(freqs[freqs["token"] == "rubén"].iloc[0]["frequency"], 4)
        self.assertEquals(freqs[freqs["token"] == "pepe"].iloc[0]["frequency"], 2)
        self.assertEquals(len(freqs.index), 3)






