#!/usr/bin/env python3
"""spark application"""

import argparse
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
    .appName("User products") \
    .getOrCreate()

# read the input file into an RDD with a record for each line
lines_RDD = spark.sparkContext.textFile(input_filepath)

user_product_RDD = lines_RDD.map(f=lambda line: line.strip().split("\t"))

# RDD (user, (product, score))
user_product_score_RDD = user_product_RDD.map(f=lambda user_product: (user_product[1], (user_product[0], user_product[2])))

# RDD (user, (products, score))
user_products_score_RDD = user_product_score_RDD.groupByKey()

# RDD (user, (products, sorted score))
output_RDD = user_products_score_RDD.map(f=lambda x: (x[0], sorted(set(x[1]), key=lambda score: score[1], reverse=True)))

# RDD (user, (top 5 product, score)) sort by key
output_five_products_RDD = output_RDD.map(f=lambda user_products: (user_products[0], list(user_products[1])[:5])).sortByKey()

output_five_products_RDD.saveAsTextFile(output_filepath)

