from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("chispa").getOrCreate()


@udf(StringType())
def get_gender(
    gender: StringType(), variation: StringType() = "default"
) -> StringType():
    if variation == "variation_2":
        gmap = {"M": "male", "F": "female", "U": "unknown"}
        if gender in gmap:
            return gmap[gender]
    elif variation == "variation_3":
        # help clean the input data, and infer the synonym for example: man/men, women/woman
        # the outputs are exactly in ['M','F']
        gender_clean = gender.upper().replace(" ", "")
        if (
            gender_clean in ["F", "FEMALE"]
            or re.search("WOM[AE]N", gender_clean)
            or re.search(r"^F\W", gender_clean)
        ):
            return "female"
        if (
            gender_clean in ["M", "MALE"]
            or re.search("M[AE]N", gender_clean)
            or re.search(r"^M\W", gender_clean)
        ):
            return "male"
    else:
        # Returns default
        gmap = {"M": "male", "F": "female"}
        if gender in gmap:
            return gmap[gender]
