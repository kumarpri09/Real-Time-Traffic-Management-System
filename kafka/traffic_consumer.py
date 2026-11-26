
import json
from kafka import KafkaConsumer

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "traffic-data"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    group_id="traffic-management-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    key_deserializer=lambda key: (
        key.decode("utf-8") if key else None
    ),
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)

print("Real-Time Traffic Consumer Started...")
print("Waiting for traffic data...\n")

try:
    for message in consumer:

        traffic = message.value

        print("=" * 55)
        print("REAL-TIME TRAFFIC INFORMATION")
        print("=" * 55)

        print("Location:", traffic["location"])
        print("Location ID:", traffic["location_id"])
        print("Timestamp:", traffic["timestamp"])

        print("Traffic Volume:", traffic["traffic_volume"])
        print("Average Speed:", traffic["avg_vehicle_speed"], "km/h")

        print("Cars:", traffic["vehicle_count_cars"])
        print("Trucks:", traffic["vehicle_count_trucks"])
        print("Bikes:", traffic["vehicle_count_bikes"])

        print("Weather:", traffic["weather_condition"])
        print("Temperature:", traffic["temperature"])
        print("Humidity:", traffic["humidity"])

        print("Accident:", traffic["accident_reported"])
        print("Signal:", traffic["signal_status"])

        print("Kafka Partition:", message.partition)
        print("Kafka Offset:", message.offset)

except KeyboardInterrupt:
    print("\nStopping traffic consumer...")

finally:
    consumer.close()
