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


def generate_column_to_select(df: DataFrame) -> list:
    column_to_select = []

    for column, dtype in df.dtypes:
        if "struct" not in dtype:
            column_name = column
            alias_name = column

            column_to_select.append(
                (column_name, alias_name)
            )

            continue

        column_name = f"{column}.true"
        alias_name = column

        tag_column_name = f"{column}._id"
        tag_alias_name = f"{column}_id"

        column_to_select += [
            (column_name, alias_name),
            (tag_column_name, tag_alias_name)
        ]

    return column_to_select


def get_curent_node_df(df: DataFrame) -> DataFrame:
    column_to_select = generate_column_to_select(df)

    df = df.selectExpr(
        [f"{column_name} as {alias_name}" for column_name, alias_name in column_to_select]
    )

    return df
