from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///code/spark-course/book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

#for key, value in wordCounts.items():
#    print(key, '=>', value)

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
