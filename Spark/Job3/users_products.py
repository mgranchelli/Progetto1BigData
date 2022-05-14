#!/usr/bin/env python3
"""spark application"""

import argparse
import itertools

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

lines_RDD = spark.sparkContext.textFile(input_filepath)

user_product_score_RDD = lines_RDD.map(f=lambda line: line.strip().split("\t"))

# Filter score < 4
user_product_filtered_score_RDD = user_product_score_RDD.filter(lambda score: (int(score[2]) >= 4))

# RDD (product, user)
product_user_RDD = user_product_filtered_score_RDD.map(f=lambda user_product: (user_product[0], user_product[1]))
# RDD (product, users)
product_users_RDD = product_user_RDD.groupByKey()

product_users_ordered_RDD = product_users_RDD.map(f=lambda product_users: (product_users[0], sorted(list(product_users[1]))))

# RDD (product, couple users)
product_couple_users_RDD = product_users_ordered_RDD.map(f=lambda product_users: (product_users[0], itertools.combinations(product_users[1], 2)))

# RDD (couple users, product)
users_product_RDD = product_couple_users_RDD.flatMap(f=lambda product_users: [(i, product_users[0]) for i in product_users[1]])

# RDD (couple users, products)
users_products_RDD = users_product_RDD.groupByKey()

# RDD remove duplicate in products
output_RDD = users_products_RDD.map(f=lambda x: (x[0], set(x[1])))

# RDD output cleaned and ordered
output_cleaned_RDD = output_RDD.filter(lambda users: (len(users[1]) >= 3)).sortByKey()

output_cleaned_RDD.saveAsTextFile(output_filepath)
