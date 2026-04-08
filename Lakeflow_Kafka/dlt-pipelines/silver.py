import dlt
from pyspark.sql.types import StructType, IntegerType, StringType
from pyspark.sql.functions import col, from_json

schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("user_id", StringType()) \
    .add("amount", IntegerType())

@dlt.table(
    name="ecommerce_silver_lakeflow",
    comment="Cleaned and structured data from Bronze layer"
)
def silver():

    df_bronze = dlt.read_stream("ecommerce_bronze_lakeflow")  # ✅ FIX

    df_silver = df_bronze.withColumn(
        "parsed",
        from_json(col("value"), schema)
    ).select(
        col("parsed.*"),
        col("kafka_timestamp"),
        col("ingestion_time")
    )

    df_silver = df_silver.filter(col("order_id").isNotNull())

    return df_silver