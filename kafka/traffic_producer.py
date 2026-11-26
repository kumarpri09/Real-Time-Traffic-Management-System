
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "traffic-data"

# Traffic locations
TRAFFIC_LOCATIONS = [
    {"id": "DELHI_001", "name": "Connaught Place"},
    {"id": "DELHI_002", "name": "India Gate"},
    {"id": "DELHI_003", "name": "Karol Bagh"},
    {"id": "DELHI_004", "name": "Dwarka"},
    {"id": "DELHI_005", "name": "Rohini"}
]

WEATHER = ["Clear", "Rainy", "Cloudy", "Foggy"]
SIGNALS = ["Red", "Yellow", "Green"]


def generate_traffic_data():

    location = random.choice(TRAFFIC_LOCATIONS)

    cars = random.randint(50, 200)
    trucks = random.randint(10, 50)
    bikes = random.randint(20, 100)

    speed = random.randint(5, 70)

    traffic_volume = cars + trucks + bikes

    accident = random.random() < 0.05

    data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location_id": location["id"],
        "location": location["name"],
        "traffic_volume": traffic_volume,
        "avg_vehicle_speed": speed,
        "vehicle_count_cars": cars,
        "vehicle_count_trucks": trucks,
        "vehicle_count_bikes": bikes,
        "weather_condition": random.choice(WEATHER),
        "temperature": random.randint(18, 42),
        "humidity": random.randint(20, 95),
        "accident_reported": accident,
        "signal_status": random.choice(SIGNALS)
    }

    return data


def main():

    print("Starting Real-Time Traffic Producer...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5
    )

    try:
        while True:

            traffic_data = generate_traffic_data()

            future = producer.send(
                TOPIC_NAME,
                key=traffic_data["location_id"],
                value=traffic_data
            )

            metadata = future.get(timeout=10)

            print("\nTraffic data sent successfully")
            print(json.dumps(traffic_data, indent=4))
            print(
                f"Topic: {metadata.topic}, "
                f"Partition: {metadata.partition}, "
                f"Offset: {metadata.offset}"
            )

            time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopping traffic producer...")

    except KafkaError as error:
        print("Kafka error:", error)

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
speed = random.randint(5, 70)

speed = 7

speed = 35


