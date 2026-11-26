
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, BooleanType
)

# 1. Get MongoDB connection string from environment
MONGODB_URI = os.environ.get("MONGODB_URI")

if not MONGODB_URI:
    raise RuntimeError("MONGODB_URI environment variable is missing")

# 2. Start Spark
spark = (
    SparkSession.builder
    .appName("SmartTrafficMongoDBIntegration")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 3. Traffic event schema
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("traffic_volume", IntegerType(), True),
    StructField("avg_vehicle_speed", DoubleType(), True),
    StructField("vehicle_count_cars", IntegerType(), True),
    StructField("vehicle_count_trucks", IntegerType(), True),
    StructField("vehicle_count_bikes", IntegerType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("accident_reported", BooleanType(), True),
    StructField("signal_status", StringType(), True)
])

# 4. Read Kafka topic
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "traffic-data")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# 5. Convert Kafka JSON into Spark columns
traffic_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_data")
    .select(from_json(col("json_data"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .withColumn("ingested_at", current_timestamp())
    .filter(
        col("location_id").isNotNull()
        & col("event_time").isNotNull()
        & col("avg_vehicle_speed").isNotNull()
        & col("traffic_volume").isNotNull()
    )
)

# 6. Add congestion classification
analyzed_df = traffic_df.withColumn(
    "congestion_level",
    when(col("avg_vehicle_speed") < 10, "SEVERE")
    .when(col("avg_vehicle_speed") < 25, "HEAVY")
    .when(col("avg_vehicle_speed") < 40, "MODERATE")
    .otherwise("LOW")
)

# 7. MongoDB batch writer
def write_to_mongodb(batch_df, batch_id, collection_name):

    if batch_df.isEmpty():
        return

    print(
        f"Writing batch {batch_id} "
        f"to MongoDB collection {collection_name}"
    )

    (
        batch_df.write
        .format("mongodb")
        .mode("append")
        .option("connection.uri", MONGODB_URI)
        .option("database", "traffic_management")
        .option("collection", collection_name)
        .save()
    )

    print(f"Batch {batch_id} saved successfully")

# 8. Store all valid traffic events
events_query = (
    analyzed_df.writeStream
    .outputMode("append")
    .foreachBatch(
        lambda df, batch_id:
        write_to_mongodb(df, batch_id, "traffic_events")
    )
    .option(
        "checkpointLocation",
        "/home/princekum/RealTimeTrafficManagement/spark/checkpoints/mongodb_events"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

# 9. Select accident and congestion alerts
alerts_df = (
    analyzed_df
    .filter(
        (col("accident_reported") == True)
        | (col("avg_vehicle_speed") < 25)
    )
    .withColumn(
        "alert_type",
        when(col("accident_reported") == True, "ACCIDENT ALERT")
        .when(col("avg_vehicle_speed") < 10, "SEVERE TRAFFIC ALERT")
        .otherwise("HEAVY TRAFFIC ALERT")
    )
)

# 10. Store alerts separately
alerts_query = (
    alerts_df.writeStream
    .outputMode("append")
    .foreachBatch(
        lambda df, batch_id:
        write_to_mongodb(df, batch_id, "traffic_alerts")
    )
    .option(
        "checkpointLocation",
        "/home/princekum/RealTimeTrafficManagement/spark/checkpoints/mongodb_alerts"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

print("Kafka to Spark to MongoDB integration started.")

spark.streams.awaitAnyTermination()
