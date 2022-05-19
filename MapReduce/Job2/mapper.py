#!/usr/bin/env python3
"""mapper.py"""

import sys

# read line from standard input
for line in sys.stdin:
    # removing leading/trailing whitespaces
    line = line.strip()
    # split the current line into productId, userId, score
    productId, userId, score = line.split("\t")

    productId = productId.strip()
    userId = userId.strip()
    score = score.strip()
    
    print('%s\t%s\t%s' % (userId, productId, score))