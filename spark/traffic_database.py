
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, window,
    count, avg, round, sum, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, BooleanType
)

# -----------------------------------
# 1. Spark configuration
# -----------------------------------

spark = (
    SparkSession.builder
    .appName("SmartTrafficPostgreSQLIntegration")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------------
# 2. PostgreSQL configuration
# -----------------------------------

JDBC_URL = "jdbc:postgresql://localhost:5432/traffic_db"

DB_PROPERTIES = {
    "user": "traffic_user",
    "password": "Traffic@123",
    "driver": "org.postgresql.Driver"
}

# -----------------------------------
# 3. Kafka JSON schema
# -----------------------------------

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

# -----------------------------------
# 4. Read traffic events from Kafka
# -----------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "traffic-data")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

traffic_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_data")
    .select(from_json(col("json_data"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .filter(
        col("location_id").isNotNull()
        & col("event_time").isNotNull()
        & col("avg_vehicle_speed").isNotNull()
        & col("traffic_volume").isNotNull()
    )
)

# -----------------------------------
# 5. Classify congestion
# -----------------------------------

analyzed_df = traffic_df.withColumn(
    "congestion_level",
    when(col("avg_vehicle_speed") < 10, "SEVERE")
    .when(col("avg_vehicle_speed") < 25, "HEAVY")
    .when(col("avg_vehicle_speed") < 40, "MODERATE")
    .otherwise("LOW")
)

# -----------------------------------
# 6. Function: write Spark batch to PostgreSQL
# -----------------------------------

def write_to_postgres(batch_df, batch_id, table_name):

    if batch_df.isEmpty():
        return

    print(f"Writing batch {batch_id} to {table_name}")

    (
        batch_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", DB_PROPERTIES["user"])
        .option("password", DB_PROPERTIES["password"])
        .option("driver", DB_PROPERTIES["driver"])
        .mode("append")
        .save()
    )

    print(f"Batch {batch_id} saved to {table_name}")

# -----------------------------------
# 7. Calculate one-minute traffic metrics
# -----------------------------------

metrics_df = (
    analyzed_df
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("location_id"),
        col("location")
    )
    .agg(
        count(lit(1)).alias("sensor_event_count"),
        round(avg("avg_vehicle_speed"), 2).alias("average_speed_kmh"),
        round(avg("traffic_volume"), 2).alias("average_traffic_volume"),
        sum("traffic_volume").alias("total_reported_vehicle_counts"),
        sum(
            when(
                col("congestion_level").isin("HEAVY", "SEVERE"), 1
            ).otherwise(0)
        ).alias("heavy_traffic_events"),
        sum(
            when(col("accident_reported") == True, 1).otherwise(0)
        ).alias("accident_events")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("location_id"),
        col("location"),
        col("sensor_event_count"),
        col("average_speed_kmh"),
        col("average_traffic_volume"),
        col("total_reported_vehicle_counts"),
        col("heavy_traffic_events"),
        col("accident_events")
    )
)

# Append mode waits for watermarked windows to finish.
metrics_query = (
    metrics_df.writeStream
    .outputMode("append")
    .foreachBatch(
        lambda df, batch_id:
        write_to_postgres(df, batch_id, "traffic_metrics")
    )
    .option(
        "checkpointLocation",
        "/home/princekum/RealTimeTrafficManagement/spark/checkpoints/postgres_metrics"
    )
    .trigger(processingTime="10 seconds")
    .start()
)

# -----------------------------------
# 8. Select accident and traffic alerts
# -----------------------------------

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
    .select(
        col("event_time"),
        col("location_id"),
        col("location"),
        col("traffic_volume").cast("double").alias("traffic_volume"),
        col("avg_vehicle_speed").alias("average_speed_kmh"),
        col("congestion_level"),
        col("alert_type"),
        col("accident_reported"),
        col("signal_status")
    )
)

alerts_query = (
    alerts_df.writeStream
    .outputMode("append")
    .foreachBatch(
        lambda df, batch_id:
        write_to_postgres(df, batch_id, "traffic_alerts")
    )
    .option(
        "checkpointLocation",
        "/home/princekum/RealTimeTrafficManagement/spark/checkpoints/postgres_alerts"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

print("Spark Kafka to PostgreSQL integration started.")

spark.streams.awaitAnyTermination()
