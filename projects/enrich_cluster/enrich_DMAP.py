from pyspark.sql.functions import *
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame


def enrich_DMAP(
    df: SparkDataFrame,
    dim_household_cluster_type_id: int,
    dim_weight_variation_id: int,
    db_name: str = "PROD",
    schema_name: str = "WEIGHTS",
    table_name: str = "DIM_HOUSEHOLD_CLUSTER",
    join_column_name: str = "DIM_HOUSEHOLD_CLUSTER_ID",
) -> SparkDataFrame:
    """
    DMAP/Cluster enrichment function

    The DMAP/Cluster data is referring to which cluster is used for market groupings. Some clusters have only one market while others have many.
    We will enrich the input dataframe with PROD.WEIGHTS.DIM_HOUSEHOLD_CLUSTER by left join.

    :param df: Input dataframe
    :param dim_household_cluster_type_id :int - Use in WHERE - PROD.WEIGHTS.DIM_HOUSEHOLD_CLUSTER_TYPE FK
    :param dim_weight_variation_id: int - Use in WHERE - PROD.WEIGHTS.DIM_WEIGHT_VARIATION FK
    :param db_name: (string - default PROD),
    :param schema_name: (string - default WEIGHTS)
    :param table_name: (string - default DIM_HOUSEHOLD_CLUSTER)
    :param join_column_name: column used for joining (string - which will be the equivalent of PROD.WEIGHTS.DIM_HOUSEHOLD_CLUSTER.ID)
    """

    user = DBUtils(spark).secrets.get(
        scope="videoamp", key="prod-snowflake-mlops-username"
    )
    password = DBUtils(spark).secrets.get(
        scope="videoamp", key="prod-snowflake-mlops-password"
    )

    options_write = {
        "sfURL": "videoamp.us-east-1.privatelink.snowflakecomputing.com",
        "sfUser": user,
        "sfPassword": password,
        "sfAccount": "videoamp",
        "sfDatabase": db_name,
        "sfSchema": schema_name,
        "sfWarehouse": "WH_ML_OPS_MEDIUM",
    }
    enrich_data = (
        spark.read.format("snowflake")
        .options(**options_write)
        .option("dbtable", table_name)
        .load()
    )

    enrich_data = enrich_data.filter(
        (col("dim_household_cluster_type_id") == dim_household_cluster_type_id)
        & (col("dim_weight_variation_id") == dim_weight_variation_id)
    ).select(
        col("ID"),
        col("NAME"),
        col("CLUSTER_HOUSEHOLD_COUNT"),
        col("ENTITY_HOUSEHOLD_COUNTS"),
        col("ENTITY_NAMES"),
        col("DIM_HOUSEHOLD_CLUSTER_TYPE_ID"),
        col("DIM_WEIGHT_VARIATION_ID"),
    ).withColumn("cluster_id",col("ID"))
    return df.join(enrich_data, join_column_name, "left")


