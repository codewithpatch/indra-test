import xml.etree.ElementTree as ET

from lib.chash import add_row_hash_to_df


def test_add_row_hash_to_df(spark, src_dir):
    # GIVEN
    node_name = "PersAutoPolicyModRq"
    src_file_path = src_dir / "xml_original_test_file.xml"

    src_xml = ET.parse(src_file_path)

    df = spark.read.format("com.databricks.spark.xml") \
        .option("rowTag", node_name) \
        .option("valueTag", True) \
        .load(str(src_file_path))


    # WHEN
    result_df = add_row_hash_to_df(df, src_xml, node_name)

    actual_row_hash = result_df.select("PK_PersAutoPolicyModRq").collect()[0][0]

    # THEN
    expected_row_hash = "18e7e5e13429119f738237689033d45a256a6f78"

    assert "PK_PersAutoPolicyModRq" in result_df.columns
    assert actual_row_hash == expected_row_hash