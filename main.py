from pyspark import SparkContext
from pyspark.shell import spark

sc = spark.sparkContext
# test.csv using spark into a DataFrame
df = spark.read.csv("test.csv", header=True, inferSchema=True)
df.show()

print()