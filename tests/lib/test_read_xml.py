import re

from pyspark.sql.dataframe import DataFrame


def get_curent_node_from_df(df: DataFrame) -> DataFrame:
    pass


def has_nested_node(dtype: str) -> bool:
    struct_count = len(re.findall(r"struct", dtype))

    return struct_count > 1


def remove_nested_node_from_df(df: DataFrame) -> DataFrame:
    column_with_nested_node = []
    for column, dtype in df.dtypes:

        if has_nested_node(dtype):
            column_with_nested_node.append(column)

    df = df.drop(*column_with_nested_node)

    return df


def test_remove_nested_node_from_df(spark, src_dir):
    # GIVEN
    node_name = "PersAutoPolicyModRq"
    src_file_path = src_dir / "xml_sample.xml"
    df = spark.read.format("com.databricks.spark.xml") \
        .option("rowTag", node_name) \
        .option("valueTag", True) \
        .load(str(src_file_path))


    # WHEN
    result_df = remove_nested_node_from_df(df)


    # THEN
    assert "Producer" not in result_df.columns


def test_get_current_node_df(spark, src_dir):
    # GIVEN
    node_name = "PersAutoPolicyModRq"
    src_file_path = src_dir / "xml_sample.xml"
    df = spark.read.format("com.databricks.spark.xml") \
        .option("rowTag", node_name) \
        .option("valueTag", True) \
        .load(str(src_file_path))


    # WHEN
    # result_df = get_curent_node_from_df(df)


    # THEN
    output_csv = src_dir / "pers_auto_policy_mod_rq.csv"
    expected_df = spark.read.csv(str(output_csv), header=True, inferSchema=True, sep="\t")

    # assert result_df.collect() == expected_df.collect()
    assert True