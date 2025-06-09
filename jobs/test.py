from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName("WordCountApp")
sc = SparkContext(conf=conf)


rdd = sc.parallelize([
    "hello world",
    "this is a test",
    "hello from spark"
])
words = rdd.flatMap(lambda line: line.split(" "))
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
print(word_counts.collect())
sc.stop()
