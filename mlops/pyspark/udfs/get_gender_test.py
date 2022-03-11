from mlops.pyspark.udfs.get_gender import get_gender
from pyspark.sql import functions as F
from chispa import assert_column_equality
import pytest
from pyspark.sql import SparkSession
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("chispa").getOrCreate()


def test_get_gender_default(spark):

    data = [
        ("John Doe", "F", "female"),
        ("Jane Eod", "M", "male"),
        ("Mary Kim", "U", None),
    ]

    df = spark.createDataFrame(data, ["name", "gender", "expected_gender"]).withColumn(
        "output_gender", get_gender(F.col("gender"))
    )
    assert_column_equality(df, "expected_gender", "output_gender")


def test_get_gender_variation2(spark):

    data = [
        ("John Doe", "F", "female"),
        ("Jane Eod", "M", "male"),
        ("Mary Kim", "U", "unknown"),
    ]

    df = spark.createDataFrame(data, ["name", "gender", "expected_gender"]).withColumn(
        "output_gender", get_gender(F.col("gender"), F.lit("variation_2"))
    )
    assert_column_equality(df, "expected_gender", "output_gender")


def test_get_gender_variation3(spark):

    data = [
        ("John Doe", "F", "female"),
        ("Jane Eod", "M", "male"),
        ("Ma Kim", " man,", "male"),
        ("Irene Kim", " women:", "female"),
        ("Wendy Lee", " f", "female"),
        ("Ben Ed", " m,", "male"),
    ]

    df = spark.createDataFrame(data, ["name", "gender", "expected_gender"]).withColumn(
        "output_gender", get_gender(F.col("gender"), F.lit("variation_3"))
    )
    assert_column_equality(df, "expected_gender", "output_gender")
