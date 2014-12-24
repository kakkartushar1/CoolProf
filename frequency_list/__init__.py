from __future__ import print_function
from __future__ import unicode_literals
__author__ = 'rubcuevas'




FREQ_FILE = "/Users/rubcuevas/coolprof/frequency_words_ES.txt"
STOPWORDS_ES_PATH = '/Users/rubcuevas/coolprof/stopwords_ES.txt'
NEW_WORDS_FILE = '/Users/rubcuevas/coolprof/new_words_ES.txt'
words = list()
stopwords = list()
with open(FREQ_FILE, 'r') as f:
    for line in f:
        for word in line.split(" "):
            word = word.strip()
            if word.isalpha():
                words.append(word)

    print(words)
    with open(STOPWORDS_ES_PATH, 'r') as g:
        for word in g:
            word = word.strip()
            if word in words:
                stopwords.append(word)

        with open(NEW_WORDS_FILE, 'w+') as h:
            for word in stopwords:
                h.write(word + "\n")







