"""
Producer: gets a random user from the Random User API and sends it to the
Kafka topic `random_names` every 10 seconds, for 2 minutes.
"""
import json
import time

import requests
from kafka import KafkaProducer

API_URL = "https://randomuser.me/api/?results=1"
KAFKA_SERVERS = ["kafka1:19092", "kafka2:19093", "kafka3:19094"]
TOPIC = "random_names"


def create_response_dict(url: str = API_URL) -> dict:
    """Calls the API and returns the first (and only) user."""
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()["results"][0]


def create_final_json(results: dict) -> dict:
    """Keeps only the 9 fields we want to store."""
    return {
        "full_name": f"{results['name']['title']}. {results['name']['first']} {results['name']['last']}",
        "gender":    results["gender"],
        "location":  f"{results['location']['street']['number']}, {results['location']['street']['name']}",
        "city":      results["location"]["city"],
        "country":   results["location"]["country"],
        # postcode is text: some countries have letters in postcodes (e.g. "M5V 2T6")
        "postcode":  str(results["location"]["postcode"]),
        "latitude":  float(results["location"]["coordinates"]["latitude"]),
        "longitude": float(results["location"]["coordinates"]["longitude"]),
        "email":     results["email"],
    }


def create_kafka_producer() -> KafkaProducer:
    """Connects to the 3 Kafka brokers."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def start_streaming(duration_seconds: int = 120, interval_seconds: int = 10) -> None:
    """Sends one NEW random user every `interval_seconds`, for `duration_seconds`."""
    producer = create_kafka_producer()
    end_time = time.time() + duration_seconds

    while time.time() < end_time:
        # Fix vs. the article: fetch a new user at each iteration
        kafka_data = create_final_json(create_response_dict())
        producer.send(TOPIC, kafka_data)
        print(f"Sent: {kafka_data['full_name']} ({kafka_data['country']})")
        time.sleep(interval_seconds)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    start_streaming()
