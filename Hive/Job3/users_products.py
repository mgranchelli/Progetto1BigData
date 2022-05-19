#!/usr/bin/env python3

import sys
import itertools
product_user = {}
user_product = {}
user_similar_taste = {}

# input comes from STDIN
# note: this is the output from the mapper
for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip()
    # parse the input elements
    productId, userId, score = line.split("\t")
    # convert count (currently a string) to int
    try:
        score = int(score)
    except ValueError:
        # count was not a number, in this case is just int
        continue
    
    if score >= 4:
        if userId not in user_product:
            user_product[userId] = set()
        user_product[userId].add(productId)
    
        if productId not in product_user:
            product_user[productId] = set()
    
        product_user[productId].add(userId)
    
for productId, usersId in sorted(product_user.items()):
    if len(usersId) > 1:
        couple_users = list(itertools.combinations(sorted(usersId), 2))
        for (user1, user2) in couple_users:
            # intersection 
            product_users_intersection = set(user_product[user1]).intersection(user_product[user2])
            # 3 items in the intersection and user not in output dictionary
            if (len(product_users_intersection) >= 3) and (((user1, user2) and (user2, user1)) not in user_similar_taste):
                user_similar_taste[(user1, user2)] = product_users_intersection

for users in sorted(user_similar_taste):
    print('%s\t%s\t%s' % (users[0], users[1], user_similar_taste[users]))