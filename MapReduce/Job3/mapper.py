#!/usr/bin/env python3
"""mapper.py"""

import sys
import re
import string
import logging

logging.info("START MAPPER PROCESS")
# as per recommendation from @freylis, compile once only
CLEANR = re.compile('<.*?>') 

def cleanhtml(raw_html): 
    return re.sub(CLEANR, ' ', raw_html)

# read line from standard input
for line in sys.stdin:

    # removing leading/trailing whitespaces
    line = line.strip()

    # split the current line into words
    year, words = line.split("\t")
    # remove html tag inside text
    words = cleanhtml(words)
    # replace dot with withespace
    words = words.replace(".", " ")
    
    # remove punctuation
    words = words.translate(str.maketrans('', '', string.punctuation))
    # remove withespace
    words = re.sub(' +', ' ', words)
    words = words.strip()
    words = words.split(" ")

    for word in words:
        # write in standard output the mapping word -> 1
        # in the form of tab-separated pairs
        print('%s\t%s\t%i' % (year, word.lower(), 1))

logging.info("END MAPPER PROCESS")