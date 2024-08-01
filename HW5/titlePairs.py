import pyspark
import re

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType 
from pyspark.sql.functions import col


# Required for spark-submit
sc = SparkContext("local", "TitlePairs")
spark = SparkSession(sc)

# A regular expression for parsing data from part1.dat
TUPLE = re.compile(r'\(\((\d+), (\d+)\), (\d+)\)')

titles_schema = StructType() \
    .add("book_id", IntegerType(), True) \
    .add("title", StringType(), True)

# A function that creates DataFrame rows from a line of text from
# part1.dat.
def parse_tuple(t: str) -> pyspark.sql.Row:
    book1, book2, frequency = TUPLE.search(t).groups()
    return pyspark.sql.Row(book1=int(book1),
              book2=int(book2),
              frequency=int(frequency))

# Read in the tab-delimited goodreads_titles.dat file.
# TODO: You need to add one thing here...
titles_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("delimiter", "\t") \
        .schema(titles_schema) \
        .load("/home/cs143/data/goodreads_titles.dat")


# Read in the file into an RDD and apply parse_tuple over the entire
# RDD.
part1_df = spark.createDataFrame(sc.textFile("/home/cs143/data/part1.dat").map(parse_tuple))


book1_df = part1_df.join(titles_df.withColumnRenamed('title', 'book1_title'), part1_df.book1 == titles_df.book_id)
book1_df = book1_df.select('book1_title', 'book2', 'frequency')
book2_df = book1_df.join(titles_df.withColumnRenamed('title', 'book2_title'), book1_df.book2 == titles_df.book_id)
book2_df = book2_df.select('book1_title', 'book2_title', 'frequency').sort(['frequency', 'book1_title', 'book2_title'], ascending=[False, True, True])

book2_df.write.option("delimiter", "\t").csv("/home/cs143/output2")