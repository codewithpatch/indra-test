from typing import Optional

from pyspark.sql import DataFrame

from .helpers import has_nested_node


def remove_nested_node_from_df(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    column_with_nested_node = []
    for column, dtype in df.dtypes:

        if has_nested_node(dtype):
            column_with_nested_node.append(column)

    nested_node_df = df.select(*column_with_nested_node)
    no_nested_node_df = df.drop(*column_with_nested_node)

    return no_nested_node_df, nested_node_df


def generate_column_to_select(df: DataFrame, node_name: Optional[str] = None) -> list:
    column_to_select = []

    for column, dtype in df.dtypes:
        if "struct" not in dtype:
            column_name = column
            alias_name = column if node_name is None else f"{node_name}{column}"

            column_to_select.append(
                (column_name, alias_name)
            )

            continue

        if has_nested_node(dtype):
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


def get_curent_node_df(df: DataFrame, node_name: Optional[str] = None) -> DataFrame:
    column_to_select = generate_column_to_select(df, node_name)

    df = df.selectExpr(
        [f"{column_name} as {alias_name}" for column_name, alias_name in column_to_select]
    )

    return df
