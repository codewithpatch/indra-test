from lib import remove_nested_node_from_df, get_curent_node_df


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

    df = remove_nested_node_from_df(df)


    # WHEN
    result_df = get_curent_node_df(df)


    # THEN
    output_csv = src_dir / "pers_auto_policy_mod_rq.csv"
    expected_df = spark.read.csv(str(output_csv), header=True, inferSchema=True, sep="\t")

    # assert result_df.collect() == expected_df.collect()
    # compare RequId column values
    assert result_df.select("RequId").collect() == expected_df.select("RequId").collect()

    # compare TransactionRequestDt column values
    assert result_df.select("TransactionRequestDt").collect() == expected_df.select("TransactionRequestDt").collect()

    # compare TransactionRequestDt_id column values
    assert result_df.select("TransactionRequestDt_id").collect() == expected_df.select("TransactionRequestDt_id").collect()

    # compare TransactionEffectiveDt column values
    assert result_df.select("TransactionEffectiveDt").collect() == expected_df.select("TransactionEffectiveDt").collect()

    # compare TransactionEffectiveDt_id column values
    assert result_df.select("TransactionEffectiveDt_id").collect() == expected_df.select("TransactionEffectiveDt_id").collect()