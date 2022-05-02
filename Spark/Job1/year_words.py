#!/usr/bin/env python3
"""spark application"""

import argparse
from ast import arg

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
    .appName("Year words") \
    .getOrCreate()

# read the input file into an RDD with a record for each line
lines_RDD = spark.sparkContext.textFile(input_filepath)

# get a new RDD where each record is a python dictionary (tweet)
# obtained by parsing a record (json string) of lines_RDD
years_RDD = lines_RDD.map(f=lambda line: line.strip().split("\t"))

print('\n', years_RDD.collect(), '\n')
# get a new RDD where each record is just the "text" field
# of a record (tweet Python dictionary) of tweets_RDD
# Rimuovere tag HTML e resto
texts_RDD = years_RDD.map(f=lambda year_text: (year_text[0], year_text[1].split(" ")))

print('\n', texts_RDD.collect(), '\n')

# get a new (flat) RDD by splitting the records of texts_RDD into words
words_RDD = texts_RDD.map(f=lambda word: (word[0], word[1], 1))

print('\n', words_RDD.collect(), '\n')

words_2_count_RDD = words_RDD.reduceByKey(func=lambda a, b: a + b)

#output_string_RDD = words_2_count_RDD.map(f=lambda word_count: '%s: %i' %(word_count[0], word_count[1]))

spark.sparkContext.parallelize([words_2_count_RDD]) \
                  .saveAsTextFile(output_filepath)

#print("\nOutput: %s\n" %output_string_RDD)