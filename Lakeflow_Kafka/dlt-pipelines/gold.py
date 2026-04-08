import dlt
from pyspark.sql.functions import col

# 🥈 Silver table (streaming source)
@dlt.view
def ecommerce_silver():
    return spark.readStream.table("workspace.streaming.ecommerce_silver_lakeflow")


# 🥇 Gold SCD Type 2 table
dlt.create_streaming_table("ecommerce_gold_scd2_lakeflow")

dlt.apply_changes(
    target = "ecommerce_gold_scd2_lakeflow",
    source = "ecommerce_silver_lakeflow",
    keys = ["order_id"],

    # 🔥 Equivalent to your kafka_timestamp logic
    sequence_by = col("kafka_timestamp"),

    # SCD Type 2
    stored_as_scd_type = "2",

    # Only track changes for these columns
    except_column_list = ["kafka_timestamp"],

    # Optional: handle deletes if needed
    # apply_as_deletes = col("op") == "DELETE"
)