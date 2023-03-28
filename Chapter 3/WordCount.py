from operator import add
lines = sc.textFile("file:/home/cloudera/spark-2.0.0-bin-hadoop2.7/README.md")
counts = lines.flatMap(lambda x: x.split(' ')) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add)
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

