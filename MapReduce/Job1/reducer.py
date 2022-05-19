#!/usr/bin/env python3
"""reducer.py"""

import sys

# this dictionary maps each word to the sum of the values
# that the mapper has computed for that word
year_words = {}

# input comes from STDIN
# note: this is the output from the mapper
for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    year, current_word, current_count = line.split("\t")

    # convert count (currently a string) to int
    try:
        current_count = int(current_count)
        year = int(year)
    except ValueError:
        # count was not a number, in this case is just int
        continue

    # initialize word that were not seen befor with 0
    if year not in year_words:
        year_words[year] = {}
    
    if current_word not in year_words[year]:
        year_words[year][current_word] = 0
    
    year_words[year][current_word] += current_count

for year, words in sorted(year_words.items(), reverse=True):
    print('%i' % (year))
    for word in sorted(words.items(), key=lambda x: x[1], reverse = True)[:10]:
        print('\t%s\t%i' % (word[0], word[1]))