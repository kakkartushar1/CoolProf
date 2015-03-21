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


def join_slugified_terms(top_terms_col_name, census_col_name, slug_col_name):
    terms = MongoClient("localhost", 27017)["cooldb"][top_terms_col_name]
    col_census = MongoClient("localhost", 27017)["cooldb"][census_col_name]
    slugified_terms = MongoClient("localhost", 27017)["cooldb"][slug_col_name]
    # for term_census in col_census.find():
    #     term_toinsert = {}
    #     for term in terms.find():
    #term_toinsert = {}
    #terms_census_list = [c["term"] for c in col_census.find()]
    for term in terms.find().batch_size(10):
        slugified_term = "".join([unidecode(c) if c != "ñ" else c for c in term["_id"]["term"]])
        term_census = col_census.find_one({"term": slugified_term})
        if term_census is not None:
            #if not "term" in term_toinsert.keys():
            #Extract the term from the slugified terms collection
            inserted_slug = slugified_terms.find_one({"term": slugified_term})

            if inserted_slug is not None:
                #term_toinsert["term"] = slugified_term
                slugified_terms.update({"term": slugified_term},
                    {"$set": {"count_users": inserted_slug["count_users"] + term["count_users"]}})
            else:
                slugified_terms.insert({"term": slugified_term, "count_users": term["count_users"],
                                        "census_probability": term_census["census_probability"]})


def insert_surnames_census_freqs(freqs_path, census_col_name):
    surnames = MongoClient("localhost", 27017)["cooldb"][census_col_name]
    #x = pd.ExcelFile("apellidos_frecuencia.xls")
    x = pd.ExcelFile(freqs_path)
    y = x.parse(x.sheet_names[0], na_values=[".."])
    z = x.parse(x.sheet_names[1], na_values=[".."])
    surnames_excel = y.append(z, ignore_index=True)
    for row in surnames_excel.iterrows():
        row = row[1]
        surname = row["Apellido"]
        primer_apellido_freq = int(row["Primer_Apellido"]) if not pd.isnull(row["Primer_Apellido"]) else 0
        segundo_apellido_freq = int(row["Segundo_Apellido"]) if not pd.isnull(row["Segundo_Apellido"]) else 0
        ambos_apellidos_freq = int(row["Ambos_Apellidos"]) if not pd.isnull(row["Ambos_Apellidos"]) else 0
        census_probability = (primer_apellido_freq + segundo_apellido_freq
                              - ambos_apellidos_freq)/TOTAL_POPULATION_SPAIN
        surnames.insert({"term": surname.lower(), "census_probability": census_probability})


def insert_names_census_freqs(freqs_path, census_col_name):
    names = MongoClient("localhost", 27017)["cooldb"][census_col_name]
    x = pd.ExcelFile(freqs_path)
    y = x.parse(x.sheet_names[0], na_values=[".."])
    for row in y.iterrows():
        row = row[1]
        name = row["Nombre"]
        freq = row["Frecuencia"]
        census_probability = freq / TOTAL_POPULATION_SPAIN
        names.insert({"term": name.lower(), "census_probability": census_probability})


def insert_compound_names_as_simple(freqs_path, db_name, census_col_name, population=TOTAL_POPULATION_SPAIN):
    names = MongoClient("localhost", 27017)[db_name][census_col_name]
    #x = pd.ExcelFile(freqs_path)
    #y = x.parse(x.sheet_names[0], na_values=[".."])
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





def insert_terms_probabilities(twitter_col_name, census_col_name):
    twitter_terms = MongoClient("localhost", 27017)["cooldb"][twitter_col_name] #slugified
    census_terms = MongoClient("localhost", 27017)["cooldb"][census_col_name]
    total_twitter_terms = twitter_terms.count()
    for twitter_term_cursor in twitter_terms.find():
        twitter_term = twitter_term_cursor["term"]
        census_term = census_terms.find_one({"term": twitter_term})
        term_count = int(twitter_term_cursor["count_users"])
        census_probability = float(census_term["census_probability"])
        twitter_probability = term_count / total_twitter_terms
        twitter_terms.update({"term": twitter_term},
                             {"$set": {"twitter_probability": twitter_probability}})
        twitter_terms.update({"term": twitter_term},
                             {"$set": {"confidence": census_probability / twitter_probability}})




def gender_label_fullname_terms(path):
    terms = MongoClient("localhost", 27017)["cooldb"]["top_fullname_terms"]
    probs = MongoClient("localhost", 27017)["cooldb"]["surname_probs"]
    x = pd.ExcelFile(path)
    surnames = pd.parse(x.sheet_names[0]).append(x.sheet_names[1])








