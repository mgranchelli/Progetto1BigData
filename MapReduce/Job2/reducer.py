#!/usr/bin/env python3
"""reducer.py"""

import sys

user_product_score = {}

# input comes from STDIN
# note: this is the output from the mapper
for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    userId, productId, score = line.split("\t")

    try:
        score = int(score)
    except ValueError:
        continue

    if userId not in user_product_score:
        user_product_score[userId] = {}
    
    user_product_score[userId][productId] = score


for userId in sorted(user_product_score.keys()):
    print('%s' % userId)
    for productID in sorted(user_product_score[userId].items(), key=lambda x: x[1], reverse = True)[:5]:
        print('\t%s\t%i' % (productID[0], productID[1]))