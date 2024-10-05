from pathlib import Path
from xml.etree import ElementTree as ET

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from lib import reader, chash, output

spark = SparkSession \
        .builder \
        .appName("indra") \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0") \
        .getOrCreate()

SRC_DIR = Path("src")
OUT_DIR = Path("output")


def run_process(df: DataFrame, xml: ET.ElementTree):

    # Root Node
    root_node = xml.getroot()
    root_node_name = root_node.tag

    current_node_df = reader.get_curent_node_df(df)
    current_node_df, _ = reader.remove_nested_node_from_df(current_node_df)
    current_node_df = chash.add_row_hash_to_df(current_node_df, xml, root_node_name)

    # Write the current node to CSV
    output_csv = output.write_df_to_csv(current_node_df, OUT_DIR, root_node_name)

    print(f"Output CSV: {output_csv}")



if __name__ == "__main__":

    source_file = "xml_sample.xml"
    source_file_path = SRC_DIR / source_file

    # Read the XML file
    xml = ET.parse(str(source_file_path))
    top_level_node = xml.getroot()

    # Read the XML file into a DataFrame
    df = spark.read.format("com.databricks.spark.xml") \
        .option("rowTag", top_level_node.tag) \
        .option("valueTag", True) \
        .load(str(source_file_path))

    run_process(df, xml)

