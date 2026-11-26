
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

locations = [
    "Connaught Place",
    "India Gate",
    "Karol Bagh",
    "Dwarka",
    "Rohini",
    "Noida Sector 18"
]

weather_conditions = ["Clear", "Rainy", "Cloudy"]

print("Smart Traffic Producer Started...")

try:
    while True:
        traffic = {
            "timestamp": datetime.now().isoformat(),
            "location_id": random.choice(locations),
            "traffic_volume": random.randint(100, 1500),
            "avg_vehicle_speed": random.randint(5, 100),
            "vehicle_count_cars": random.randint(50, 800),
            "vehicle_count_trucks": random.randint(10, 200),
            "vehicle_count_bikes": random.randint(20, 500),
            "weather_condition": random.choice(weather_conditions),
            "temperature": random.randint(15, 40),
            "humidity": random.randint(20, 90),
            "accident_reported": random.choice([True, False]),
            "signal_status": random.choice(
                ["RED", "GREEN", "YELLOW"]
            )
        }

        producer.send("traffic-data", value=traffic)
        producer.flush()

        print("Traffic Event:", traffic)

        time.sleep(2)

except KeyboardInterrupt:
    print("Producer stopped.")

finally:
    producer.close()
