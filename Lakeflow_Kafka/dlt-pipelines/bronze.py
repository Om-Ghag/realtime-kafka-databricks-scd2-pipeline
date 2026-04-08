import dlt
from pyspark.sql.functions import current_timestamp

from config.kafka_secrets import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_API_KEY,
    KAFKA_API_SECRET
)

api_key = KAFKA_API_KEY
api_secret = KAFKA_API_SECRET
bootstrap_server = KAFKA_BOOTSTRAP_SERVERS

# Kafka JAAS config
jaas_config = f"""
kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required
username="{api_key}"
password="{api_secret}";
"""

# ✅ Bronze Table (Streaming)
@dlt.table(
    name="ecommerce_bronze_lakeflow",
    comment="Raw ingestion from Kafka with metadata"
)
def bronze():

    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe", "ecommerce_v4")
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .load()
    )

    df_bronze = df_kafka.selectExpr(
        "CAST(key AS STRING) as key",
        "CAST(value AS STRING) as value",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp"
    ).withColumn("ingestion_time", current_timestamp())

    return df_bronze