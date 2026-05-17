"""
streams_processor.py — Faust Streams Processor
Consumes messages from 'raw-data', runs the pre-trained Random Forest model
on each record, and publishes predictions to the 'predictions' topic.

Usage:
    faust -A streams_processor worker -l info

This uses Faust's @app.agent decorator — NOT a plain consumer loop.
Each message flows through a proper Faust stream topology:
  raw-data topic → agent → ML inference → predictions topic
"""

import os
import json
import joblib
import numpy as np
import faust
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")

MODEL_PATH = os.path.join(os.path.dirname(__file__), "models", "co_model.joblib")
SCALER_PATH = os.path.join(os.path.dirname(__file__), "models", "scaler.joblib")
FEATURES_PATH = os.path.join(os.path.dirname(__file__), "models", "feature_names.joblib")

# ── Load model artifacts once at startup ──────────────────────────────────────
print("[Processor] Loading ML model ...")
model = joblib.load(MODEL_PATH)
scaler = joblib.load(SCALER_PATH)
FEATURES = joblib.load(FEATURES_PATH)
print(f"[Processor] Model ready. Features: {FEATURES}")

# ── Faust app configuration ────────────────────────────────────────────────────
broker_kwargs = {}
if KAFKA_USERNAME and KAFKA_PASSWORD:
    broker_credentials = faust.SASLCredentials(
        username=KAFKA_USERNAME,
        password=KAFKA_PASSWORD,
        ssl=True,
    )
    broker_kwargs["broker_credentials"] = broker_credentials

app = faust.App(
    "air-quality-processor",
    broker=f"kafka://{KAFKA_BOOTSTRAP_SERVERS}",
    value_serializer="json",
    **broker_kwargs,
)

# ── Topic definitions ──────────────────────────────────────────────────────────
raw_topic = app.topic("raw-data", value_type=bytes)
predictions_topic = app.topic("predictions", value_type=bytes)


# ── Faust record schema (optional typing) ─────────────────────────────────────
class SensorRecord(faust.Record, serializer="json"):
    date: str = ""
    time: str = ""
    co_actual: float = 0.0
    # sensor columns (will be accessed dynamically)


# ── Faust Agent (the Streams topology) ────────────────────────────────────────
@app.agent(raw_topic)
async def process_sensor_stream(stream):
    """
    Faust agent that defines the stream topology:
      raw-data → extract features → ML predict → publish to predictions
    """
    async for record in stream:
        try:
            # Support both dict (native JSON) and bytes
            if isinstance(record, bytes):
                data = json.loads(record)
            else:
                data = record

            # Extract feature vector
            feature_vector = np.array([[data[f] for f in FEATURES]])
            feature_scaled = scaler.transform(feature_vector)

            # Run ML inference
            prediction = float(model.predict(feature_scaled)[0])
            actual = data.get("co_actual", None)

            # Build output message
            output = {
                "date": data.get("date", ""),
                "time": data.get("time", ""),
                "predicted_CO_mg_m3": round(prediction, 3),
                "actual_CO_mg_m3": round(actual, 3) if actual is not None else None,
                "error_mg_m3": round(abs(prediction - actual), 3) if actual is not None else None,
                "temperature_C": data.get("T"),
                "humidity_pct": data.get("RH"),
                "benzene_C6H6": data.get("C6H6(GT)"),
                "sensor_CO": data.get("PT08.S1(CO)"),
            }

            # Send to predictions topic
            await predictions_topic.send(value=json.dumps(output).encode())

            print(
                f"[Processor] {output['date']} {output['time']} | "
                f"Predicted CO: {prediction:.3f} mg/m³ | "
                f"Actual: {actual:.3f} mg/m³ | "
                f"Error: {output['error_mg_m3']:.3f}"
            )

        except Exception as e:
            print(f"[Processor] ERROR processing record: {e}")


if __name__ == "__main__":
    app.main()
