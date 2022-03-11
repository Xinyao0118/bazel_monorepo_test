import pytest
from pyspark_test import assert_pyspark_df_equal
from pyspark.sql import SparkSession
from enrich_DMAP import enrich_DMAP

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

def test_enrich_DMAP(spark):

    data = [
        (1, "digital", "1"),
        (2, "linear", "2"),
        (1, "digital", "3"),
    ]

    base = spark.createDataFrame(data, ["ID", "Type", "index"])
    comp = enrich_DMAP(base,1,0,join_column_name="ID")

    assert_pyspark_df_equal(base,base)
