#!/usr/bin/env python3
"""reducer.py"""

import sys

user_product_score = {}

for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    userId, productId, score = line.split("\t")

    try:
        score = int(score)
    except ValueError:
        continue

    # initialize user product score of userId that were not seen befor with 0
    if userId not in user_product_score:
        user_product_score[userId] = {}
    
    user_product_score[userId][productId] = score


for userId in sorted(user_product_score.keys()):
    print('%s' % userId)
    for productID in sorted(user_product_score[userId].items(), key=lambda x: x[1], reverse = True)[:5]:
        print('\t%s\t%i' % (productID[0], productID[1]))