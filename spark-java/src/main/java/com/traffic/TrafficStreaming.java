
package com.traffic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

public class TrafficStreaming {

    private static final String KAFKA_SERVERS = "localhost:9092";
    private static final String TOPIC = "traffic-data";

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("RealTimeSmartTrafficManagement")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "3")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("======================================");
        System.out.println(" SMART TRAFFIC MANAGEMENT - SPARK");
        System.out.println(" Kafka: " + KAFKA_SERVERS);
        System.out.println(" Topic: " + TOPIC);
        System.out.println("======================================");

        // 1. Define the incoming Kafka JSON schema.

        StructType trafficSchema = new StructType()
                .add("timestamp", DataTypes.StringType)
                .add("location_id", DataTypes.StringType)
                .add("location", DataTypes.StringType)
                .add("traffic_volume", DataTypes.IntegerType)
                .add("avg_vehicle_speed", DataTypes.IntegerType)
                .add("vehicle_count_cars", DataTypes.IntegerType)
                .add("vehicle_count_trucks", DataTypes.IntegerType)
                .add("vehicle_count_bikes", DataTypes.IntegerType)
                .add("weather_condition", DataTypes.StringType)
                .add("temperature", DataTypes.IntegerType)
                .add("humidity", DataTypes.IntegerType)
                .add("accident_reported", DataTypes.BooleanType)
                .add("signal_status", DataTypes.StringType);

        // 2. Read events from Kafka.

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_SERVERS)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load();

        // 3. Convert Kafka value from binary to JSON string.

        Dataset<Row> jsonStream = kafkaStream
                .selectExpr("CAST(value AS STRING) AS json_value");

        // 4. Parse the JSON into structured columns.

        Dataset<Row> trafficStream = jsonStream
                .select(from_json(
                        col("json_value"),
                        trafficSchema
                ).as("traffic"))
                .select("traffic.*");

        // 5. Convert event timestamp and classify congestion.

        Dataset<Row> analyzedStream = trafficStream
                .withColumn(
                        "event_time",
                        to_timestamp(col("timestamp"))
                )
                .withColumn(
                        "congestion_level",
                        when(col("avg_vehicle_speed").isNull(), "UNKNOWN")
                        .when(col("avg_vehicle_speed").lt(10), "SEVERE")
                        .when(col("avg_vehicle_speed").lt(25), "HEAVY")
                        .when(col("avg_vehicle_speed").lt(40), "MODERATE")
                        .otherwise("LOW")
                )
                .withColumn(
                        "alert_type",
                        when(col("accident_reported").equalTo(true),
                                "ACCIDENT")
                        .when(col("avg_vehicle_speed").lt(25),
                                "HIGH_CONGESTION")
                        .otherwise("NORMAL")
                );

        // 6. Output individual traffic events.

        Dataset<Row> eventOutput = analyzedStream.select(
                col("event_time"),
                col("location_id"),
                col("location"),
                col("traffic_volume"),
                col("avg_vehicle_speed"),
                col("congestion_level"),
                col("weather_condition"),
                col("accident_reported"),
                col("signal_status"),
                col("alert_type")
        );

        StreamingQuery eventQuery = eventOutput.writeStream()
                .queryName("traffic_events")
                .format("console")
                .outputMode("append")
                .option("truncate", "false")
                .option("numRows", "20")
                .option(
                        "checkpointLocation",
                        "/home/princekum/RealTimeTrafficManagement/spark-java/checkpoints/events"
                )
                .start();

        // 7. Calculate one-minute window analytics per location.

        Dataset<Row> metrics = analyzedStream
                .filter(col("event_time").isNotNull())
                .withWatermark("event_time", "2 minutes")
                .groupBy(
                        window(col("event_time"), "1 minute"),
                        col("location_id"),
                        col("location")
                )
                .agg(
                        count("*").alias("sensor_event_count"),
                        round(avg("avg_vehicle_speed"), 2)
                                .alias("average_speed_kmh"),
                        round(avg("traffic_volume"), 2)
                                .alias("average_traffic_volume"),
                        sum("traffic_volume")
                                .alias("total_reported_vehicle_counts"),
                        sum(
                                when(col("congestion_level")
                                        .isin("SEVERE", "HEAVY"), 1)
                                .otherwise(0)
                        ).alias("heavy_traffic_events"),
                        sum(
                                when(col("accident_reported")
                                        .equalTo(true), 1)
                                .otherwise(0)
                        ).alias("accident_events")
                );

        StreamingQuery metricsQuery = metrics.writeStream()
                .queryName("traffic_metrics")
                .format("console")
                .outputMode("update")
                .option("truncate", "false")
                .option("numRows", "50")
                .option(
                        "checkpointLocation",
                        "/home/princekum/RealTimeTrafficManagement/spark-java/checkpoints/metrics"
                )
                .start();

        System.out.println("Spark streaming queries started.");

        spark.streams().awaitAnyTermination();
    }
}
