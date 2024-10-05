from pathlib import Path

from lib import remove_nested_node_from_df, get_curent_node_df
from lib.output import write_df_to_csv


def test_write_df_to_csv(spark, src_dir: Path, output_dir: Path):
    # GIVEN
    node_name = "PersAutoPolicyModRq"
    src_file_path = src_dir / "xml_sample.xml"
    df = spark.read.format("com.databricks.spark.xml") \
        .option("rowTag", node_name) \
        .option("valueTag", True) \
        .load(str(src_file_path))

    # WHEN
    df = remove_nested_node_from_df(df)
    df = get_curent_node_df(df)

    output_csv = write_df_to_csv(df, output_dir, node_name)

    # THEN
    assert output_csv.exists()