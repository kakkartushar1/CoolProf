__author__ = 'rubcuevas'

import pandas as pd
from collections import defaultdict
from pymongo import MongoClient


def write_freqs_to_file(freqs, file_path):
    with open(file_path, 'w') as f:
        f.write("token;frequency\n")
        for token, freq in freqs.iteritems():
            f.write(token + ";" + str(freq) + "\n")


def tokens_freq(server, port, dbname, dbcol, num_min_users):
    cooltweets = MongoClient(server, port)[dbname][dbcol]
    tokfreqs = pd.Series()
    tokusers = defaultdict(set)
    names = cooltweets.find({}, {"_id": 0, "name": 1, "user.name": 1, "user.screen_name": 1})
    for entry in names:
        name = entry["user"]["name"]
        user = entry["user"]["screen_name"]
        # Get tokens by replacing non-alphanumeric characters by spaces and splitting
        for token in "".join([c if c.isalnum() else " " for c in name.lower()]).split():
            # Discard tokens with numbers in them
            if all([c.isalpha() for c in token]):
                tokusers[token].add(user)
                if token in tokfreqs:
                    tokfreqs[token] += 1
                else:
                    tokfreqs[token] = 1

    return pd.Series({
        token: freq for token, freq in tokfreqs.iteritems() if len(tokusers[token]) >= num_min_users})







