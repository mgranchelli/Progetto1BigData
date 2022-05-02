#!/usr/bin/env python3
"""reducer.py"""

import sys
import itertools

# this dictionary maps each word to the sum of the values
# that the mapper has computed for that word
product_user_score = {}
user_product_score = {}
user_similar_taste = {}

# input comes from STDIN
# note: this is the output from the mapper
for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    userId, productId, score = line.split("\t")
    #print(type(userId), type(productId), type(score))
    # convert count (currently a string) to int
    try:
        score = int(score)
    except ValueError:
        # count was not a number, in this case is just int
        continue

    # initialize word that were not seen befor with 0
    if userId not in user_product_score:
        user_product_score[userId] = set()
#    user_product_score[userId][productId] = score
    user_product_score[userId].add(productId)
    
    if productId not in product_user_score:
        product_user_score[productId] = set()
    #product_user_score[productId][userId] = score
    
    product_user_score[productId].add(userId)

for productId, usersId in product_user_score.items():
    if len(usersId) > 1:
        couple_users = list(itertools.combinations(usersId, 2))
        for (user1, user2) in couple_users:
            # Insersezione tra due liste
            product_users_intersection = set(user_product_score[user1]).intersection(user_product_score[user2])
            # Più di 3 elementi nell'intersezione e utenti non presenti nella lista di output
            if (len(product_users_intersection) > 1) and (((user1, user2) and (user2, user1)) not in user_similar_taste):
                user_similar_taste[(user1, user2)] = product_users_intersection

for users in sorted(user_similar_taste):
    print('%s\t%s' % (users, user_similar_taste[users]))