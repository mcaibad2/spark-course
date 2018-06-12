from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("AmountSpentByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerId = int(fields[0])
    amountSpent = float(fields[2])
    return (customerId, amountSpent)

lines = sc.textFile("file:///code/spark-course/customer-orders.csv")
rdd = lines.map(parseLine)
amounts = rdd.reduceByKey(lambda x, y: x + y)
amountsSorted = amounts.map(lambda x: (x[1], x[0])).sortByKey()
results = amountsSorted.collect()

for result in results:
    print(result)





