__author__ = 'rubcuevas'

import pandas as pd
from collections import defaultdict
from pymongo import MongoClient
import re


STOP_WORDS = """de
la
que
el
en
y
a
los
del
se
las
por
un
para
con
no
una
su
al
es
lo
como
más
pero
sus
le
ya
o
fue
este
ha
sí
porque
esta
son
entre
está
cuando
muy
sin
sobre
ser
tiene
también
me
hasta
hay
donde
han
quien
están
estado
desde
todo
nos
durante
estados
todos
uno
les
ni
contra
otros
fueron
ese
eso
había
ante
ellos
e
esto
mí
antes
algunos
qué
unos
yo
otro
otras
otra
él
tanto
esa
estos
mucho
quienes
nada
muchos
cual
sea
poco
ella
estar
haber
estas
estaba
estamos
algunas
algo
nosotros
mi
mis
tú
te
ti
tu
tus
ellas
nosotras
vosotros
os
mío
mía
míos
mías
tuyo
tuya
tuyos
suyo
suya
suyos
nuestro
nuestra
nuestros
nuestras
vuestro
vuestra
vuestros
esos
esas
estoy
estás
está
estamos
estáis
están
esté
estés
estemos
estén
estaré
estarás
estará
estaremos
estarán
estaría
estarías
estaríamos
estarían
estaba
estabas
estábamos
estaban
estuve
estuviste
estuvo
estuvimos
estuvieron
estuviera
estuvieras
estando
estado
estados
he
has
ha
hemos
habéis
han
haya
hayas
hayamos
hayan
habrá
habrán
habría
habrías
habrían
había
habías
habíamos
habían
hubo
hubiera
hubieras
hubieran
hubiese
habido
soy
eres
es
somos
sois
son
sea
seas
seamos
sean
seré
serás
será
seremos
serán
sería
serías
serían
era
eras
éramos
eran
fui
fuiste
fue
fuimos
fueron
fuera
fueras
fueran
fuese
siendo
sido
tengo
tienes
tiene
tenemos
tenéis
tienen
tenga
tengas
tengamos
tengan
tendré
tendrás
tendrá
tendremos
tendrán
tendría
tendrías
tendríamos
tenía
tenías
teníamos
tenían
tuve
tuviste
tuvo
tuvimos
tuvieron
tuviera
tuvieras
teniendo
tenido
""".split("\n")

ADDITIONAL_TERMS="""twitter
com
http
https
"""



def is_latin(token):
    if token == re.sub(r'[^\x00-\x7F\x80-\xFF\u0100-\u017F\u0180-\u024F\u1E00-\u1EFF]', '', token):
        return True
    return False


def is_valid(token):
    return is_latin(token) and token.isalpha()


def top_terms_to_collection(field, server, port, dbname, dbcol, terms_col_name, remove_stop_words):
    cooltweets = MongoClient(server, port)[dbname][dbcol]
    coolterms = MongoClient(server, port)[dbname][terms_col_name]
    rows = cooltweets.find({}, {"_id": 0, "name": 1, "user.name": 1, "user.screen_name": 1, "user.description": 1})
    for entry in rows:
        data = entry["user"][field]
        user = entry["user"]["screen_name"]
        if data is not None:
            # Get tokens by replacing non-alphanumeric characters by spaces and splitting
            for token in "".join([c if c.isalnum() else " " for c in data.lower()]).split():
                # Discard tokens with numbers in them and stop words
                if len(token) > 1 and is_valid(token) and (not remove_stop_words or token not in STOP_WORDS):
                    term = {"term": token, "user": user}
                    coolterms.insert(term)

def clean_terms(server, port, dbname, colname, terms_to_remove):
    terms = MongoClient(server, port)[dbname][colname]
    [terms.remove({"_id.term": token}) for token in terms_to_remove]



def aggregate_terms(server, port, dbname, terms_col_name, filtered_col_name, num_min_users):
    coolterms = MongoClient(server, port)[dbname][terms_col_name]
    coolterms.aggregate([
                    {
                    "$group": {
                        "_id": {
                            "term": "$term"
                        },
                        "users": {"$addToSet": "$user"},
                        "count": {"$sum": 1}
                    }},
                    {
                    "$project":
                        {
                        "count": "$count",
                        "count_users": {"$size": "$users"}
                    }},
                    {
                    "$match":
                        {
                        "count_users": {"$gte": num_min_users}
                    }},
                    {
                        "$out": filtered_col_name
                    }
                    ],
                     allowDiskUse=True)



def top_terms_to_csv(freqs, file_path):
    with open(file_path, 'w') as f:
        f.write("token;frequency\n")
        for token, freq in freqs.iteritems():
            f.write(token + ";" + str(freq) + "\n")


def top_terms_user_data(field, server, port, dbname, dbcol, num_min_users, remove_stop_words):
    cooltweets = MongoClient(server, port)[dbname][dbcol]
    tokfreqs = pd.Series()
    tokusers = defaultdict(set)
    rows = cooltweets.find({}, {"_id": 0, "name": 1, "user.name": 1, "user.screen_name": 1, "user.description": 1})
    for entry in rows:
        data = entry["user"][field]
        user = entry["user"]["screen_name"]
        # Get tokens by replacing non-alphanumeric characters by spaces and splitting
        for token in "".join([c if c.isalnum() else " " for c in data.lower()]).split():
            # Discard tokens with numbers in them and stop words
            if token.isalpha() and (not remove_stop_words or token not in STOP_WORDS):
                #tokusers[token].add(user)

                if token in tokfreqs:
                    tokfreqs[token] += 1
                else:
                    tokfreqs[token] = 1

    return pd.Series({
        token: freq for token, freq in tokfreqs.iteritems() if len(tokusers[token]) >= num_min_users})


def full_name_terms(server, port, dbname, dbcol):
    #return top_terms_user_data("name", server, port, dbname, dbcol, num_min_users, False)
    top_terms_to_collection("name", server, port, dbname, dbcol, "fullname_terms", True)


def bio_terms(server, port, dbname, dbcol):
    top_terms_to_collection("description", server, port, dbname, dbcol, "bio_terms", True)









