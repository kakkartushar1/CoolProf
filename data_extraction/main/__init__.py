__author__ = 'rubcuevas'

import sys
sys.path.append("..")

from lib import coolfunctions as cf

cf.full_name_terms("localhost", 27017, "cooldb", "cooltweets")
print("Fullname terms extraction finished")
cf.aggregate_terms("localhost", 27017, "cooldb", "fullname_terms", "top_fullname_terms", 5)
print("Fullname terms aggregation finished")
cf.bio_terms("localhost", 27017, "cooldb", "cooltweets")
print("bio terms extraction finished")
cf.aggregate_terms("localhost", 27017, "cooldb", "bio_terms", "top_bio_terms", 5)
print("bio terms aggregation finished")