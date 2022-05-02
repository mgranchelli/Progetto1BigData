#!/usr/bin/env python3

import string
import sys
import re
# as per recommendation from @freylis, compile once only
CLEANR = re.compile('<.*?>') 

def cleanhtml(raw_html): 
    return re.sub(CLEANR, ' ', raw_html)

# read input from stdin
for line in sys.stdin:

    try:
        # remove whitespaces and trailing characters
        line = line.strip()

        # parse name and unix date using TAB as a separator
        year, words = line.split("\t")

        # try to convert the unix date to an integer
        try:
            year = int(year)
        except ValueError:
            continue

        # remove html tag inside text
        words = cleanhtml(words)
        # replace dot with withespace
        words = words.replace(".", " ")
    
        # remove punctuation
        words = words.translate(str.maketrans('', '', string.punctuation))
        words = re.sub(' +', ' ', words)
        words = words.strip()
        #  splittare riga in words per il campo text
        words = words.split(" ")
        
        # print output items to stdout, using TAB as a separator
        for word in words:
            print("%i\t%s\t%i" % (year, word.lower(), 1))

        
    except:
        import sys
        print(sys.exc_info())
