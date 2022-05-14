#!/usr/bin/env python3
"""spark application"""

import argparse
import time

from pyspark.sql import SparkSession


# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Similar users taste") \
    .getOrCreate()

def users_products_intersect(l1, l2):
    if len(set(l1).intersection(l2)) > 1:
        return set(l1).intersection(l2)

start_time = time.time()
lines_RDD = spark.sparkContext.textFile(input_filepath)

user_product_score_RDD = lines_RDD.map(f=lambda line: line.strip().split("\t"))
# Filter score < 4
user_product_filtered_score_RDD = user_product_score_RDD.filter(lambda score: (int(score[2]) >= 4))

# RDD (user, product)
user_product_RDD = user_product_filtered_score_RDD.map(f=lambda user_product: (user_product[1], user_product[0]))

# RDD (user, products)
user_products_RDD = user_product_RDD.groupByKey()

# RDD cartesian product
users_products_RDD = user_products_RDD.cartesian(user_products_RDD).filter(lambda x: x[0][0] != x[1][0] and users_products_intersect(x[0][1], x[1][1]))

# RDD (user1, user2, products_intersection)
output_RDD = users_products_RDD.map(f=lambda x: ((x[0][0], x[1][0]), users_products_intersect(x[0][1], x[1][1])))

output_cleaned_RDD = output_RDD.filter(lambda x: hash(x[0][0]) > hash(x[0][1])).sortByKey('ascending')

for key, value in output_cleaned_RDD.collect():
    print(key, list(value))

#output_cleaned_RDD.saveAsTextFile("")
end_time = time.time()
print("\nExecution time: {} seconds\n".format(end_time - start_time))