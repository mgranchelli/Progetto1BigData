#!/usr/bin/env python3
"""spark application"""

import argparse
import string
import re

from pyspark.sql import SparkSession


def get_clean_text(line):
    CLEANR = re.compile('<.*?>') 
    line = re.sub(CLEANR, ' ', line)
    line = line.replace(".", " ")
    # remove punctuation
    line = line.translate(str.maketrans('', '', string.punctuation))
    line = re.sub(' +', ' ', line)
    cleaned_line = line.strip()

    return cleaned_line


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
texts_RDD = years_RDD.map(f=lambda year_text: (year_text[0], get_clean_text(year_text[1]).split(" ")))

#test = test.flatMap(lambda xs: [(x[0], x[1]) for x in xs])

print('\n', texts_RDD.collect(), '\n')

# get a new (flat) RDD by splitting the records of texts_RDD into words
#words_RDD = texts_RDD.flatMap(f=lambda year : [((year[0], i), 0) for i in year[1]])
words_RDD = texts_RDD.flatMap(f=lambda year : [((year[0], i.lower()), 1) for i in year[1]])
#words_RDD = texts_RDD.flatMap(f=lambda year : [(year[0], (i, 1)) for i in year[1]])

print('\n', words_RDD.collect(), '\n')

#print('\n', words_RDD.groupByKey().collect(), '\n')

words_2_count_RDD = words_RDD.reduceByKey(func=lambda a, b: a + b)

#print('\n', words_2_count_RDD.takeOrdered(10, key = lambda x: (x[0][0], x[1])), '\n')
#myRDD.sortByKey(ascending=True).values().collect()
ordered_words_2_count_RDD = words_2_count_RDD.sortBy(lambda x: (x[0][0], x[1]), ascending=False)
#ordered_words_2_count_RDD = words_2_count_RDD.sortBy(lambda x: (x[0], x[1][1])[:5], ascending=False)
#print('\n', ordered_words_2_count_RDD.collect(), '\n')

RDD = ordered_words_2_count_RDD.map(f=lambda x : (x[0][0], (x[0][1], x[1])))
words_2_count_RDD = RDD.groupByKey()
#for key, value in ordered_words_2_count_RDD.items():
#    print(key, value)
print('\n')
dizionario = words_2_count_RDD.collect()
for key, value in dizionario:
    print(key, list(value)[:10])
print('\n')
#print('\n', ordered_words_2_count_RDD.flatMap(f=lambda x : x).collect(), '\n')

#print('\n', ordered_words_2_count_RDD.flatMap(f=lambda year: [((year[0], i), 1) for i in year[1]]).collect(), '\n')

#output_string_RDD = words_2_count_RDD.map(f=lambda word_count: '%s: %i' %(word_count[0], word_count[1]))

#spark.sparkContext.parallelize([words_2_count_RDD]) \
 #                 .saveAsTextFile(output_filepath)

#print("\nOutput: %s\n" %output_string_RDD)