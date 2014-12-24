__author__ = 'rubcuevas'
from data_extraction import coolfunctions as cf
from pymongo import MongoClient
import unittest
import pandas as pd

HOST = "localhost"
PORT = 27017
DBNAME = "test_cooldb"
COLNAME = "test_cooltweets"
TOKENS_FREQ_FILE = "/Users/rubcuevas/coolprof/tokens_freq.csv"
NUM_MIN_USERS = 2
tweets = [{"user": {"name": "Rubén-Cuevas Ruben9", "screen_name": "rubcuevas"}},
                    {"user": {"name": "Rubén      Cuevas", "screen_name": "rubcuevas"}},
                    {"user": {"name": "Rubén", "screen_name": "rubcuevas"}},
                    {"user": {"name": " Rubén\u2318", "screen_name": "rcuevas33"}},
                    {"user": {"name": "Pepe ", "screen_name": "kiki"}},
                    {"user": {"name": " Pepe ", "screen_name": "lulu"}},
                    {"user": {"name": "Ruben", "screen_name": "rubcuevas"}},
                    {"user": {"name": "Ruben", "screen_name": "rubengarcia"}},
                    {"user": {"name": "¿¿¿Ruben???...Ruben..", "screen_name": "garciaruben"}},
                    {"user": {"name": "Ruben", "screen_name": "tututu"}},
                    {"user": {"name": "\u2318 ¿!//Ruben??!", "screen_name": "messi"}},
                    {"user": {"name": "Mario \u2318", "screen_name": "mmario"}},
                    {"user": {"name": "Jeje \u2318", "screen_name": "00mario00"}},
                    {"user": {"name": "Mario", "screen_name": "mmario"}},
                    {"user": {"name": "D4niel", "screen_name": "dani1"}},
                    {"user": {"name": "D4niel", "screen_name": "dani2"}},
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


    def test_tokens_freq(self):
        tokens_freq = cf.tokens_freq(HOST, PORT, DBNAME, COLNAME, NUM_MIN_USERS)
        solution = {
            "rubén": 4,
            "pepe": 2,
            "ruben": 6

        }
        self.assertEquals(solution, tokens_freq.to_dict())

    def test_write_tokens_freq(self):
        cf.write_freqs_to_file(cf.tokens_freq(
           HOST, PORT, DBNAME, COLNAME, NUM_MIN_USERS
        ), TOKENS_FREQ_FILE)
        freqs = pd.read_csv(TOKENS_FREQ_FILE, sep=";")
        self.assertEquals(freqs[freqs["token"] == "ruben"].iloc[0]["frequency"], 6)
        self.assertEquals(freqs[freqs["token"] == "rubén"].iloc[0]["frequency"], 4)
        self.assertEquals(freqs[freqs["token"] == "pepe"].iloc[0]["frequency"], 2)
        self.assertEquals(len(freqs.index), 3)




