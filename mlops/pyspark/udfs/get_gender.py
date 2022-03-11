from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from re import search
GMAP_VARIATION2 = {"M": "male", "F": "female", "U": "unknown"}
GMAP_DEFAULT = {"M": "male", "F": "female"}


@udf(StringType())
def get_gender(
    gender: StringType(), variation: StringType() = "default"
) -> StringType():
    if variation == "variation_2":
        if gender in GMAP_VARIATION2:
            return GMAP_VARIATION2[gender]
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
        if gender in GMAP_DEFAULT:
            return GMAP_DEFAULT[gender]
