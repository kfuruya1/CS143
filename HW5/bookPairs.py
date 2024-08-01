from pyspark import SparkContext
sc = SparkContext("local", "bookPairs")

from itertools import combinations

def pair_split(row):
	temp = combinations(row.split(","), 2)
	result = [x for x in temp]
	return result

lines = sc.textFile("/home/cs143/data/goodreads.dat")
books = lines.flatMap(lambda line: line.split(":"))
books = books.filter(lambda line: "," in line)
books = books.flatMap(lambda line: pair_split(line))
books = books.map(lambda book_pairs: (int(book_pairs[0]), int(book_pairs[1])))
books = books.map(lambda book_pairs: (book_pairs, 1))
bookCounts = books.reduceByKey(lambda a, b: a+b)
bookCounts = bookCounts.filter(lambda kvp: kvp[1] > 20)
bookCounts.saveAsTextFile("/home/cs143/output1")
