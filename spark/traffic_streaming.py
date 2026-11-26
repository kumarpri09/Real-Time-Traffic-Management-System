
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    when,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType
)

# ----------------------------------
# 1. Kafka configuration
# ----------------------------------

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "traffic-data"

# ----------------------------------
# 2. Spark configuration
# ----------------------------------

spark = (
    SparkSession.builder
    .appName("RealTimeSmartTrafficManagement")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("\n====================================")
print(" REAL-TIME TRAFFIC MANAGEMENT SYSTEM")
print("====================================")
print("Spark Version:", spark.version)
print("Kafka Server:", KAFKA_SERVER)
print("Kafka Topic:", KAFKA_TOPIC)

# ----------------------------------
# 3. Define traffic JSON schema
# ----------------------------------

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

# ----------------------------------
# 4. Read streaming data from Kafka
# ----------------------------------

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# ----------------------------------
# 5. Convert Kafka JSON into columns
# ----------------------------------

traffic_data = (
    kafka_stream
    .select(
        from_json(
            col("value").cast("string"),
            traffic_schema
        ).alias("traffic")
    )
    .select("traffic.*")
    .filter(col("location_id").isNotNull())
)

# ----------------------------------
# 6. Detect traffic congestion
# ----------------------------------

analyzed_traffic = (
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
    .withColumn(
        "processing_time",
        current_timestamp()
    )
)

# ----------------------------------
# 7. Generate traffic alerts
# ----------------------------------

traffic_alerts = (
    analyzed_traffic
    .withColumn(
        "alert",
        when(
            col("accident_reported") == True,
            "ACCIDENT ALERT"
        )
        .when(
            col("congestion_level") == "SEVERE",
            "SEVERE TRAFFIC ALERT"
        )
        .when(
            col("congestion_level") == "HEAVY",
            "HEAVY TRAFFIC ALERT"
        )
        .otherwise("NORMAL TRAFFIC")
    )
)

# ----------------------------------
# 8. Select output columns
# ----------------------------------

final_output = traffic_alerts.select(
    "timestamp",
    "location_id",
    "location",
    "traffic_volume",
    "avg_vehicle_speed",
    "congestion_level",
    "vehicle_count_cars",
    "vehicle_count_trucks",
    "vehicle_count_bikes",
    "weather_condition",
    "accident_reported",
    "signal_status",
    "alert",
    "processing_time"
)

# ----------------------------------
# 9. Display real-time results
# ----------------------------------

query = (
    final_output.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", "20")
    .option(
        "checkpointLocation",
        os.path.expanduser(
            "~/RealTimeTrafficManagement/spark/checkpoints/traffic"
        )
    )
    .start()
)

print("\nSpark Streaming Started Successfully!")
print("Waiting for Kafka traffic events...\n")

# ----------------------------------
# 10. Keep streaming active
# ----------------------------------

try:
    query.awaitTermination()

except KeyboardInterrupt:
    print("\nStopping Spark Streaming...")

finally:
    query.stop()
    spark.stop()
    print("Spark application stopped.")
