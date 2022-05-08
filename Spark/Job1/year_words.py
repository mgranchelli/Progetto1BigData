#!/usr/bin/env python3
"""spark application"""

import argparse
import string
import time
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

start_time = time.time()
# read the input file into an RDD with a record for each line
lines_RDD = spark.sparkContext.textFile(input_filepath)

year_text_RDD = lines_RDD.map(f=lambda line: line.strip().split("\t"))

# Aggiungere convertitore di data?
year_cleaned_text_RDD = year_text_RDD.map(f=lambda year_text: (year_text[0], get_clean_text(year_text[1]).split(" ")))

# RDD ((year, word), 1)
year_words_RDD = year_cleaned_text_RDD.flatMap(f=lambda year : [((year[0], i.lower()), 1) for i in year[1]])
# RDD ((year, word), sum(1))
year_words_sum_RDD = year_words_RDD.reduceByKey(func=lambda a, b: a + b)

# ordered RDD
year_words_sum_ordered_RDD = year_words_sum_RDD.sortBy(lambda x: (x[0][0], x[1]), ascending=False)

# ordered RDD (year, (word, sum(1)))
output_RDD = year_words_sum_ordered_RDD.map(f=lambda x : (x[0][0], (x[0][1], x[1])))
output_year_list_words_RDD = output_RDD.groupByKey()

output_year_top10_words_RDD = output_year_list_words_RDD.map(f=lambda x : (x[0], list(x[1])[:10]))

for key, value in output_year_top10_words_RDD.collect():
    print(key, list(value))

#output_year_top10_words_RDD.saveAsTextFile("")
end_time = time.time()
print("\nExecution time: {} seconds\n".format(end_time - start_time))
