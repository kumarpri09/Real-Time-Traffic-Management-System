
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType
)

spark = SparkSession.builder \
    .appName("SmartTrafficStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("location_id", StringType()),
    StructField("location", StringType()),
    StructField("traffic_volume", IntegerType()),
    StructField("avg_vehicle_speed", IntegerType()),
    StructField("vehicle_count_cars", IntegerType()),
    StructField("vehicle_count_trucks", IntegerType()),
    StructField("vehicle_count_bikes", IntegerType()),
    StructField("weather_condition", StringType()),
    StructField("temperature", IntegerType()),
    StructField("humidity", IntegerType()),
    StructField("accident_reported", BooleanType()),
    StructField("signal_status", StringType())
])

print("Starting Smart Traffic Spark Streaming...")

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-data") \
    .option("startingOffsets", "latest") \
    .load()

traffic = kafka_stream.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

analysis = traffic.withColumn(
    "congestion_status",
    when(
        col("avg_vehicle_speed") < 20,
        "HIGH CONGESTION"
    ).when(
        col("traffic_volume") > 1000,
        "HEAVY TRAFFIC"
    ).otherwise("NORMAL")
).withColumn(
    "processed_at",
    current_timestamp()
)

query = analysis.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/spark-traffic-checkpoint") \
    .start()

query.awaitTermination()
