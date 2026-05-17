"""
producer.py — Air Quality Kafka Producer
Reads rows from AirQualityUCI.csv and publishes each as a JSON message
to the 'raw-data' Kafka topic at ~1 row/second.

Usage:
    python producer.py
"""

import json
import time
import csv
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")
TOPIC = "raw-data"
DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "AirQualityUCI.csv")
DELAY_SECONDS = 1.0  # ~1 row per second

FEATURES = [
    "PT08.S1(CO)", "C6H6(GT)", "PT08.S2(NMHC)", "NOx(GT)",
    "PT08.S3(NOx)", "NO2(GT)", "PT08.S4(NO2)", "PT08.S5(O3)", "T", "RH", "AH"
]


def build_producer():
    kwargs = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "key_serializer": lambda k: k.encode("utf-8") if k else None,
    }
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        kwargs.update({
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": KAFKA_USERNAME,
            "sasl_plain_password": KAFKA_PASSWORD,
        })
    return KafkaProducer(**kwargs)


def parse_row(row: dict) -> dict | None:
    """Parse a CSV row into a numeric feature dict. Returns None if invalid."""
    try:
        record = {
            "date": row.get("Date", ""),
            "time": row.get("Time", ""),
            "co_actual": float(row["CO(GT)"]),
        }
        for feat in FEATURES:
            val = row[feat].replace(",", ".")
            record[feat] = float(val)
        return record
    except (ValueError, KeyError):
        return None


def main():
    print(f"[Producer] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} ...")
    producer = build_producer()
    print(f"[Producer] Connected. Streaming '{DATA_FILE}' → topic '{TOPIC}'")
    print(f"[Producer] Press Ctrl+C to stop.\n")

    sent = 0
    skipped = 0

    with open(DATA_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for i, row in enumerate(reader):
            record = parse_row(row)
            if record is None:
                skipped += 1
                continue

            key = f"row-{i}"
            producer.send(TOPIC, key=key, value=record)
            sent += 1

            print(
                f"[Producer] Sent row {i:>5} | "
                f"Date: {record['date']} {record['time']} | "
                f"CO(actual): {record['co_actual']:.1f} mg/m³ | "
                f"Temp: {record['T']:.1f}°C"
            )
            time.sleep(DELAY_SECONDS)

    producer.flush()
    print(f"\n[Producer] Done. Sent: {sent}, Skipped (bad rows): {skipped}")


if __name__ == "__main__":
    main()
