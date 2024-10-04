import re

from pyspark.sql import DataFrame


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
