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
cin.insert_compound_names_as_simple(freqs_path="cleaned/nombres_simples_frecuencia_hombres",
                                    db_name="cooldb", census_col_name="man_names")