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

def read_xml_to_df(filepath: Path, node_name: str):
    df = spark.read.format("com.databricks.spark.xml") \
        .option("rowTag", node_name) \
        .option("valueTag", True) \
        .load(str(filepath))

    return df


def run_process(child_node_df: DataFrame, xml: ET.ElementTree):

    ##### Root Node
    root_node = xml.getroot()
    root_node_name = root_node.tag

    current_node_df = reader.get_curent_node_df(child_node_df)
    current_node_df, _ = reader.remove_nested_node_from_df(current_node_df)
    current_node_df, parent_node_hash = chash.add_row_hash_to_df(current_node_df, xml, root_node_name)

    # Write the current node to CSV
    output_csv = output.write_df_to_csv(current_node_df, OUT_DIR, root_node_name)

    print(f"Output CSV: {output_csv}")

    ##### Child Nodes
    for child_node in root_node:
        child_node_xml = ET.ElementTree(child_node)
        child_node_df = read_xml_to_df(SRC_DIR / "xml_sample.xml", child_node.tag)
        child_node_name = child_node.tag

        child_node_df = reader.get_curent_node_df(child_node_df, child_node_name)
        child_node_df, _ = reader.remove_nested_node_from_df(child_node_df)
        child_node_df, child_node_hash = chash.add_row_hash_to_df(
            child_node_df, child_node_xml, child_node_name, parent_node_hash
        )

        # Write the child node to CSV
        output_csv = output.write_df_to_csv(child_node_df, OUT_DIR, child_node_name)

        print(f"Output CSV: {output_csv}")



if __name__ == "__main__":

    source_file = "xml_sample.xml"
    source_file_path = SRC_DIR / source_file

    # Read the XML file
    xml = ET.parse(str(source_file_path))
    top_level_node = xml.getroot()

    # Read the XML file into a DataFrame
    df = read_xml_to_df(source_file_path, top_level_node.tag)

    run_process(df, xml)

