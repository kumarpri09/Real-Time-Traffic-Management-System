
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    avg,
    sum,
    count,
    when,
    round,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType
)

# ==========================================
# 1. CONFIGURATION
# ==========================================

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "traffic-data"

PROJECT_DIR = os.path.expanduser(
    "~/RealTimeTrafficManagement/spark"
)

# ==========================================
# 2. CREATE SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("SmartTrafficAdvancedAnalytics")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("======================================")
print(" SMART TRAFFIC ADVANCED ANALYTICS")
print("======================================")
print("Spark Version:", spark.version)

# ==========================================
# 3. DEFINE TRAFFIC DATA SCHEMA
# ==========================================

traffic_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("traffic_volume", IntegerType(), True),
    StructField("avg_vehicle_speed", IntegerType(), True),
    StructField("vehicle_count_cars", IntegerType(), True),
    StructField("vehicle_count_trucks", IntegerType(), True),
    StructField("vehicle_count_bikes", IntegerType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("accident_reported", BooleanType(), True),
    StructField("signal_status", StringType(), True)
])

# ==========================================
# 4. READ DATA FROM KAFKA
# ==========================================

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# ==========================================
# 5. CONVERT JSON TO SPARK DATAFRAME
# ==========================================

traffic_data = (
    kafka_stream
    .select(
        from_json(
            col("value").cast("string"),
            traffic_schema
        ).alias("traffic")
    )
    .select("traffic.*")
    .filter(
        col("location_id").isNotNull() &
        col("avg_vehicle_speed").isNotNull() &
        col("traffic_volume").isNotNull()
    )
    .withColumn(
        "event_time",
        to_timestamp(col("timestamp"))
    )
    .filter(col("event_time").isNotNull())
)

# ==========================================
# 6. CLASSIFY TRAFFIC CONDITIONS
# ==========================================

analyzed_data = (
    traffic_data
    .withColumn(
        "congestion_level",
        when(
            col("avg_vehicle_speed") < 10,
            "SEVERE"
        )
        .when(
            col("avg_vehicle_speed") < 25,
            "HEAVY"
        )
        .when(
            col("avg_vehicle_speed") < 40,
            "MODERATE"
        )
        .otherwise("LOW")
    )
)

# ==========================================
# 7. ONE-MINUTE WINDOW ANALYTICS
# ==========================================

windowed_data = (
    analyzed_data
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("location_id"),
        col("location")
    )
    .agg(
        count(lit(1)).alias("sensor_event_count"),

        round(
            avg("avg_vehicle_speed"), 2
        ).alias("average_speed_kmh"),

        round(
            avg("traffic_volume"), 2
        ).alias("average_traffic_volume"),

        sum("traffic_volume").alias(
            "total_reported_vehicle_counts"
        ),

        sum(
            when(
                col("congestion_level").isin(
                    "HEAVY", "SEVERE"
                ),
                1
            ).otherwise(0)
        ).alias("heavy_traffic_events"),

        sum(
            when(
                col("accident_reported") == True,
                1
            ).otherwise(0)
        ).alias("accident_events")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "location_id",
        "location",
        "sensor_event_count",
        "average_speed_kmh",
        "average_traffic_volume",
        "total_reported_vehicle_counts",
        "heavy_traffic_events",
        "accident_events"
    )
)

# ==========================================
# 8. DISPLAY LOCATION-WISE ANALYTICS
# ==========================================

analytics_query = (
    windowed_data.writeStream
    .queryName("TrafficWindowAnalytics")
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .option("numRows", "50")
    .option(
        "checkpointLocation",
        PROJECT_DIR + "/checkpoints/analytics"
    )
    .trigger(processingTime="10 seconds")
    .start()
)

# ==========================================
# 9. GENERATE REAL-TIME ALERTS
# ==========================================

alerts = (
    analyzed_data
    .filter(
        (col("accident_reported") == True) |
        (col("avg_vehicle_speed") < 25)
    )
    .withColumn(
        "alert_type",
        when(
            col("accident_reported") == True,
            "ACCIDENT ALERT"
        )
        .when(
            col("avg_vehicle_speed") < 10,
            "SEVERE TRAFFIC ALERT"
        )
        .otherwise("HEAVY TRAFFIC ALERT")
    )
    .select(
        "timestamp",
        "location_id",
        "location",
        "avg_vehicle_speed",
        "traffic_volume",
        "congestion_level",
        "accident_reported",
        "alert_type"
    )
)

# ==========================================
# 10. DISPLAY REAL-TIME ALERTS
# ==========================================

alert_query = (
    alerts.writeStream
    .queryName("TrafficAlertMonitoring")
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", "50")
    .option(
        "checkpointLocation",
        PROJECT_DIR + "/checkpoints/alerts"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

print("\nAdvanced traffic analytics started!")
print("One-minute window aggregation: ACTIVE")
print("Traffic alert monitoring: ACTIVE")
print("Waiting for Kafka events...")

# ==========================================
# 11. KEEP STREAMING RUNNING
# ==========================================

try:
    spark.streams.awaitAnyTermination()

except KeyboardInterrupt:
    print("\nStopping traffic analytics...")

finally:
    for query in spark.streams.active:
        query.stop()

    spark.stop()

    print("Spark analytics stopped.")
