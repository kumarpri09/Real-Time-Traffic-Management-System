
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "traffic-data",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="traffic-analysis-group",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)

print("Smart Traffic Consumer Started...")

for message in consumer:
    traffic = message.value

    print("\n========== TRAFFIC ANALYSIS ==========")
    print("Location:", traffic["location_id"])
    print("Traffic Volume:", traffic["traffic_volume"])
    print("Average Speed:", traffic["avg_vehicle_speed"])
    print("Weather:", traffic["weather_condition"])
    print("Accident:", traffic["accident_reported"])
    print("Signal:", traffic["signal_status"])

    if traffic["traffic_volume"] > 1000:
        print("ALERT: HIGH TRAFFIC DETECTED!")

    if traffic["avg_vehicle_speed"] < 20:
        print("WARNING: TRAFFIC CONGESTION!")

    if traffic["accident_reported"]:
        print("EMERGENCY: ACCIDENT REPORTED!")

    print("======================================")
