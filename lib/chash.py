import hashlib
from xml.etree import ElementTree as ET

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType


def get_hash_from_xml(xml: ET.ElementTree) -> str:
    xml_str = ET.tostring(xml.getroot())

    return hashlib.sha1(xml_str).hexdigest()


def add_row_hash_to_df(df: DataFrame, xml: ET.ElementTree, node_name: str) -> DataFrame:
    row_hash_name = f"PK_{node_name}"
    row_hash_udf = get_hash_from_xml(xml)

    df = df.withColumn(row_hash_name, lit(row_hash_udf).cast(StringType()))

    return df
