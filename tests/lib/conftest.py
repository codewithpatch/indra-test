from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def src_dir() -> Path:
    return Path('../test_file')


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    return SparkSession \
        .builder \
        .appName("indra_unittest") \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0") \
        .getOrCreate()