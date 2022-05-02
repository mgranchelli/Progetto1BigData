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

# get a new RDD where each record is a python dictionary (tweet)
# obtained by parsing a record (json string) of lines_RDD
users_RDD = lines_RDD.map(f=lambda line: line.strip().split("\t"))

print('\n', users_RDD.collect(), '\n')
# get a new RDD where each record is just the "text" field
# of a record (tweet Python dictionary) of tweets_RDD
# Rimuovere tag HTML e resto
users_RDD = users_RDD.map(f=lambda user_product: (user_product[1], (user_product[0], user_product[2])))

#test = test.flatMap(lambda xs: [(x[0], x[1]) for x in xs])

print('\n', users_RDD.collect(), '\n')

#words_2_count_RDD = users_RDD.reduceByKey(func=lambda a, b: (a[0], a[1], b[0], b[1]))

words_2_count_RDD = users_RDD.groupByKey()

#print('\n', words_2_count_RDD.collect(), '\n')

for key, value in words_2_count_RDD.sortByKey('ascending').collect():
    print(key, list(value)[:2])



#print('\n', ordered_words_2_count_RDD.flatMap(f=lambda x : x).collect(), '\n')

#print('\n', ordered_words_2_count_RDD.flatMap(f=lambda year: [((year[0], i), 1) for i in year[1]]).collect(), '\n')

#output_string_RDD = words_2_count_RDD.map(f=lambda word_count: '%s: %i' %(word_count[0], word_count[1]))

#spark.sparkContext.parallelize([words_2_count_RDD]) \
 #                 .saveAsTextFile(output_filepath)

#print("\nOutput: %s\n" %output_string_RDD)