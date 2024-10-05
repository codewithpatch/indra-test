import hashlib
from typing import Optional
from xml.etree import ElementTree as ET

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

from lib.model import ColumnNodeHash


def get_hash_from_xml(xml: ET.ElementTree) -> str:
    xml_str = ET.tostring(xml.getroot())

    return hashlib.sha1(xml_str).hexdigest()


def add_row_hash_to_df(
        df: DataFrame,
        xml: ET.ElementTree, node_name: str,
        parent_hash: Optional[ColumnNodeHash] = None
) -> tuple[DataFrame, ColumnNodeHash]:
    row_hash_name = f"PK_{node_name}"
    row_hash = get_hash_from_xml(xml)

    df = df.withColumn(row_hash_name, lit(row_hash).cast(StringType()))

    if parent_hash:
        df = df.withColumn(parent_hash.column_name, lit(parent_hash.hash_value).cast(StringType()))

    return df, ColumnNodeHash(row_hash_name, row_hash)
