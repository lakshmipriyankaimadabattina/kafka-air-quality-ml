"""
consumer.py — Output Consumer
Reads from the 'predictions' Kafka topic and prints each prediction
to the console in a readable, formatted style as it arrives.

Usage:
    python consumer.py
"""

import json
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")
TOPIC = "predictions"

HEADER = """
╔══════════════════════════════════════════════════════════════╗
║       REAL-TIME AIR QUALITY CO PREDICTION CONSUMER          ║
║       Dataset: UCI Air Quality  |  Model: Random Forest      ║
╚══════════════════════════════════════════════════════════════╝
"""

ROW_FMT = (
    "┌─ {date} {time}\n"
    "│  Predicted CO : {predicted:>7.3f} mg/m³\n"
    "│  Actual CO    : {actual:>7.3f} mg/m³\n"
    "│  Error        : {error:>7.3f} mg/m³\n"
    "│  Temperature  : {temp:>6.1f} °C   Humidity: {rh:.1f}%\n"
    "│  Benzene C6H6 : {benzene:>6.1f} µg/m³   Sensor CO: {sensor_co}\n"
    "└──────────────────────────────────────────────────────────"
)


def build_consumer():
    kwargs = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "auto_offset_reset": "latest",
        "enable_auto_commit": True,
        "group_id": "output-consumer-group",
        "value_deserializer": lambda v: json.loads(v.decode("utf-8")),
    }
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        kwargs.update({
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": KAFKA_USERNAME,
            "sasl_plain_password": KAFKA_PASSWORD,
        })
    return KafkaConsumer(TOPIC, **kwargs)


def display(record: dict):
    actual = record.get("actual_CO_mg_m3")
    predicted = record.get("predicted_CO_mg_m3", 0.0)
    error = record.get("error_mg_m3")

    print(ROW_FMT.format(
        date=record.get("date", "N/A"),
        time=record.get("time", "N/A"),
        predicted=predicted,
        actual=actual if actual is not None else 0.0,
        error=error if error is not None else 0.0,
        temp=record.get("temperature_C") or 0.0,
        rh=record.get("humidity_pct") or 0.0,
        benzene=record.get("benzene_C6H6") or 0.0,
        sensor_co=record.get("sensor_CO") or "N/A",
    ))


def main():
    print(HEADER)
    print(f"[Consumer] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} ...")
    consumer = build_consumer()
    print(f"[Consumer] Listening on topic '{TOPIC}' ... (Ctrl+C to stop)\n")

    count = 0
    try:
        for msg in consumer:
            count += 1
            record = msg.value
            print(f"\n[#{count}]")
            display(record)
    except KeyboardInterrupt:
        print(f"\n[Consumer] Stopped after {count} predictions.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
