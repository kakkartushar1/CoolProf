__author__ = 'rubcuevas'

import pandas as pd
import numpy as np
from pymongo import MongoClient
from unidecode import unidecode
TOTAL_POPULATION_SPAIN = 46464053



def read_data_by_nationality(path):
    x = pd.ExcelFile(path)
    df = pd.DataFrame()
    for k in range(0, len(x.sheet_names)):
        t = x.parse(x.sheet_names[k], header=None)
        countries = []
        colnames = []
        for i in range(0, len(t.columns)):
            if i % 3 == 0:
                countries.append(t[i][0])
                countries.append(t[i][0])
                countries.append(t[i][0])
            if i in [0, 1, 2]:
                colnames.append(t[i][1])

        cols = colnames * len(countries)
        array = np.array([countries, cols])
        t.columns = pd.MultiIndex.from_arrays(array)
        df = pd.concat([df, t[2:]], axis=1)
    return df


def insert_data_by_nationality(path, census_col_name):
    df = read_data_by_nationality(path)





def get_total_freq_surnames():
    terms = MongoClient("localhost", 27017)["cooldb"]["top_fullname_terms"]
    surnames = MongoClient("localhost", 27017)["cooldb"]["surnames"]
    count = 0
    for surname in surnames.find():
        term = terms.find({"id.term": surname})
        if term is not None:
            count += term["count_users"]
    return count


def slugify_and_join_terms(dbname, top_terms_col_name, result_col_name):
    terms = MongoClient("localhost", 27017)[dbname][top_terms_col_name]
    #col_census = MongoClient("localhost", 27017)[dbname][census_col_name]
    slugified_terms = MongoClient("localhost", 27017)[dbname][result_col_name]
    # for term_census in col_census.find():
    #     term_toinsert = {}
    #     for term in terms.find():
    #term_toinsert = {}
    #terms_census_list = [c["term"] for c in col_census.find()]
    for term in terms.find().batch_size(10):
        slugified_term = "".join([unidecode(c) if c != "ñ" else c for c in term["_id"]["term"]]).lower()
        # term_census = col_census.find_one({"term": slugified_term})
        # if term_census is not None:
        #     #if not "term" in term_toinsert.keys():
        #     #Extract the term from the slugified terms collection
        inserted_slug = slugified_terms.find_one({"term": slugified_term})

        if inserted_slug is not None:
            #term_toinsert["term"] = slugified_term
            slugified_terms.update({"term": slugified_term},
                {"$set": {"count_users": inserted_slug["count_users"] + term["count_users"]}})
        else:
            slugified_terms.insert({"term": slugified_term, "count_users": term["count_users"]})

def extract_terms(dbname, twitter_col_name, census_col_name, result_col_name):
    twitter_col = MongoClient("localhost", 27017)[dbname][twitter_col_name]
    census_col = MongoClient("localhost", 27017)[dbname][census_col_name]
    result_col = MongoClient("localhost", 27017)[dbname][result_col_name]
    for row in census_col.find().batch_size(10):
        twitter_term = twitter_col.find_one({"term": row["term"]})
        if twitter_term is not None:
            result_col.insert({"term": row["term"], "census_probability": float(row["census_probability"]),
                               "count_users": int(twitter_term["count_users"])})




def insert_surnames_census_freqs(freqs_path, dbname, census_col_name, population=TOTAL_POPULATION_SPAIN):
    surnames = MongoClient("localhost", 27017)[dbname][census_col_name]
    #x = pd.ExcelFile("apellidos_frecuencia.xls")
    surnames_excel = pd.read_excel(freqs_path, na_values=[".."], keep_default_na=False, parse_cols=3)
    for row in surnames_excel.iterrows():
        row = row[1]
        simple_surnames = row["Apellido"].lower().split(" ")
        primer_apellido_freq = int(row["Primer_Apellido"]) if not pd.isnull(row["Primer_Apellido"]) else 0
        segundo_apellido_freq = int(row["Segundo_Apellido"]) if not pd.isnull(row["Segundo_Apellido"]) else 0
        ambos_apellidos_freq = int(row["Ambos_Apellidos"]) if not pd.isnull(row["Ambos_Apellidos"]) else 0
        freq = primer_apellido_freq + segundo_apellido_freq - ambos_apellidos_freq
        census_probability = (freq/population)
        for surname in simple_surnames:
            surname_already_inserted = surnames.find_one({"term": surname})

            if surname_already_inserted is not None:
                current_freq = float(surname_already_inserted["census_probability"]) * population
                surnames.update({"term": surname}, {"$set": {"census_probability": (current_freq + freq) / population}})
            else:
                surnames.insert({"term": surname, "census_probability": census_probability})


def insert_names_census_freqs(freqs_path, db_name, census_col_name, population=TOTAL_POPULATION_SPAIN):
    names = MongoClient("localhost", 27017)[db_name][census_col_name]
    y = pd.read_excel(freqs_path, na_values=[".."], keep_default_na=False, parse_cols=4)
    for row in y.iterrows():
        row = row[1]
        simple_names = row["Nombre"].split(" ")
        freq = row["Frecuencia"]
        census_probability = freq / population
        for simple_name in simple_names:
            simple_name = simple_name.lower()
            name_already_inserted = names.find_one({"term": simple_name})
            if name_already_inserted is not None:
                current_freq = float(name_already_inserted["census_probability"]) * population
                names.update({"term": simple_name},
                             {"$set": {"census_probability": (current_freq + freq) / population}})
            else:
                names.insert({"term": simple_name, "census_probability": freq / population})


def count_different_users(dbname, colname):
    col = MongoClient("localhost", 27017)[dbname][colname]
    count = 0
    for row in col.find().batch_size(10):
        count += row["count_users"]
    return count


def insert_terms_probabilities(dbname, twitter_col_name, census_col_name, result_col_name):
    twitter_terms = MongoClient("localhost", 27017)[dbname][twitter_col_name] #slugified
    census_terms = MongoClient("localhost", 27017)[dbname][census_col_name]
    result_col = MongoClient("localhost", 27017)[dbname][result_col_name]
    total_twitter_terms = count_different_users(dbname, twitter_col_name)
    # for twitter_term_cursor in twitter_terms.find():
    #     twitter_term = twitter_term_cursor["term"]
    #     census_term = census_terms.find_one({"term": twitter_term})
    #     term_count = int(twitter_term_cursor["count_users"])
    #     census_probability = float(census_term["census_probability"])
    #     twitter_probability = term_count / total_twitter_terms
    #     twitter_terms.update({"term": twitter_term},
    #                          {"$set": {"twitter_probability": twitter_probability}})
    #     twitter_terms.update({"term": twitter_term},
    #                          {"$set": {"confidence": census_probability / twitter_probability}})
    for census_obj in census_terms.find().batch_size(10):
        census_term = census_obj["term"]
        twitter_term_obj = twitter_terms.find_one({"term": census_term})
        twitter_term_count = int(twitter_term_obj["count_users"])
        census_prob = float(census_obj["census_probability"])
        twitter_prob = twitter_term_count / total_twitter_terms
        result_col.insert({"term": census_term, "census_probability": census_prob, "twitter_probability": twitter_prob,
                           "confidence": census_prob / twitter_prob})



def gender_label_fullname_terms(path):
    terms = MongoClient("localhost", 27017)["cooldb"]["top_fullname_terms"]
    probs = MongoClient("localhost", 27017)["cooldb"]["surname_probs"]
    x = pd.ExcelFile(path)
    surnames = pd.parse(x.sheet_names[0]).append(x.sheet_names[1])








