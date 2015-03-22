__author__ = 'rubcuevas'

import sys
sys.path.append("..")

from lib import coolfunctions as cf
from lib import cooline as cin
#
# cf.full_name_terms("localhost", 27017, "cooldb", "cooltweets")
# print("Fullname terms extraction finished")
# cf.aggregate_terms("localhost", 27017, "cooldb", "fullname_terms", "top_fullname_terms", 5)
# print("Fullname terms aggregation finished")
# cf.bio_terms("localhost", 27017, "cooldb", "cooltweets")
# print("bio terms extraction finished")
# cf.aggregate_terms("localhost", 27017, "cooldb", "bio_terms", "top_bio_terms", 5)
# print("bio terms aggregation finished")
# cin.insert_surnames_census_freqs("resources/cleaned_census_datasets/apellidos_frecuencia.xls",
#                                  "cooldb", "surnames_census")
# cin.insert_compound_names_as_simple(freqs_path="cleaned/nombres_simples_frecuencia_hombres",
#                                     db_name="cooldb", census_col_name="man_names")

cin.slugify_and_join_terms(dbname="cooldb", top_terms_col_name="top_fullname_terms",
                           result_col_name="slugified_fullname_terms")
print("Joining finished")
cin.extract_terms(dbname="cooldb", twitter_col_name="slugified_fullname_terms",
                  census_col_name="surnames_census", result_col_name="extracted_surnames")
print("Surnames extracted")
cin.extract_terms(dbname="cooldb", twitter_col_name="slugified_fullname_terms",
                  census_col_name="man_names_census", result_col_name="extracted_man_names")
print("Man names extracted")
cin.extract_terms(dbname="cooldb", twitter_col_name="slugified_fullname_terms",
                  census_col_name="woman_names_census", result_col_name="extracted_woman_names")
print("Woman names extracted")

# cin.insert_terms_probabilities("cooldb", "slugified_terms", "surnames_census", "surnames_stats")
# cin.insert_terms_probabilities("cooldb", "man_names_census", "")