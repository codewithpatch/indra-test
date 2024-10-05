from pathlib import Path

# from pyspark import SparkContext
# from pyspark.shell import spark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


spark = SparkSession \
        .builder \
        .appName("indra") \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0") \
        .getOrCreate()
# sc = spark.sparkContext
# sc.install_maven_artifact("com.databricks:spark-xml_2.12:0.12.0")
# test.csv using spark into a DataFrame
# df = spark.read.csv("test.csv", header=True, inferSchema=True)
# df.show()
#
# print()

SRC = Path("SRC")
source_file = "xml_original_test_file.xml"
source_file_path = SRC / source_file

# Read the XML file into a DataFrame
df = spark.read.format("com.databricks.spark.xml") \
    .option("rowTag", "PersAutoPolicyModRq") \
    .option("valueTag", True) \
    .load(str(source_file_path))

# flatten semi-structured data

# Show the DataFrame

df.show()