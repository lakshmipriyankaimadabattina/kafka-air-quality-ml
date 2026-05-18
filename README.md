# Real-Time Air Quality Streaming with Apache Kafka
**ENGR 5785G — Assignment 1**

## Overview

This project builds a real-time ML inference pipeline using Apache Kafka and Faust Streams. Rows from the UCI Air Quality dataset are streamed through Kafka at ~1 row/second, processed by a pre-trained Random Forest model, and predictions are printed live to a consumer terminal.

---

## Dataset

**UCI Air Quality Dataset**
- Source: [archive.ics.uci.edu/dataset/360](https://archive.ics.uci.edu/dataset/360)
- Records: 9,357 hourly measurements from an Italian city (March 2004 – February 2005)
- **ML Task:** Regression — predict CO concentration (mg/m³) from metal oxide sensor readings, temperature, humidity, and benzene levels
- Features used: `PT08.S1(CO)`, `C6H6(GT)`, `PT08.S2(NMHC)`, `NOx(GT)`, `PT08.S3(NOx)`, `NO2(GT)`, `PT08.S4(NO2)`, `PT08.S5(O3)`, `T`, `RH`, `AH`

---

## Streams Library

**Python + Faust** (`faust-streaming`)

The processor uses Faust's `@app.agent` decorator to define a proper streams topology:

```
raw-data topic → Faust Agent → Random Forest inference → predictions topic
```

This is **not** a plain consumer loop — Faust handles the consumer group, offset management, and async stream processing internally.

---

## ML Model

| Property | Value |
|---|---|
| Algorithm | Random Forest Regressor |
| Trees | 100 |
| Target | CO concentration (mg/m³) |
| MAE | **0.3132 mg/m³** |
| R² | **0.9044** |

The model is trained offline (`train_model.py`) and loaded once at Faust processor startup. Every incoming sensor record is scaled and fed to the model; the prediction is published to the `predictions` topic.

---

## Architecture

```
┌──────────────┐   raw-data   ┌─────────────────────┐  predictions  ┌──────────────────┐
│  producer.py │ ──────────▶  │ streams_processor.py │ ────────────▶ │   consumer.py    │
│              │   (Kafka)    │  (Faust @app.agent)  │   (Kafka)     │  prints to CLI   │
│  Reads CSV   │              │  Random Forest model │               │                  │
└──────────────┘              └─────────────────────┘               └──────────────────┘
```

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/lakshmipriyankaimadabattina/kafka-ml-pipeline.git
cd kafka-ml-pipeline
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Start Kafka (local Docker)

```bash
docker compose up -d
```

Or use [Confluent Cloud](https://confluent.cloud) (free tier). Copy `.env.example` to `.env` and fill in your credentials.

### 4. Train the model (one-time)

```bash
python train_model.py
```

This saves `models/co_model.joblib`, `models/scaler.joblib`, and `models/feature_names.joblib`.

---

## Running the Pipeline

Open **three terminals** side by side and run in this order:

### Terminal 1 — Faust Streams Processor
```bash
faust -A streams_processor worker -l info
```

### Terminal 2 — Producer
```bash
python producer.py
```

### Terminal 3 — Output Consumer
```bash
python consumer.py
```

Wait 5–10 seconds for Faust to initialize, then start the producer. Predictions will appear in Terminal 3 in real time.

---

## Project Structure

```
kafka-ml-pipeline/
├── producer.py           # Reads CSV → publishes to raw-data topic
├── streams_processor.py  # Faust agent: raw-data → ML predict → predictions topic
├── consumer.py           # Reads predictions topic → prints to console
├── train_model.py        # Offline training script (run once)
├── data/
│   └── AirQualityUCI.csv # Dataset
├── models/
│   ├── co_model.joblib   # Trained Random Forest model
│   ├── scaler.joblib     # Fitted StandardScaler
│   └── feature_names.joblib
├── docker-compose.yml    # Local Kafka + Zookeeper
├── .env.example          # Kafka connection config template
├── requirements.txt
└── README.md
```

---

## Video Demo
https://drive.google.com/file/d/19HJUl9mwl07WASNs5-F5ELimEHxwC03Y/view?usp=sharing
---

## Notes

- The producer replays historical rows at 1 row/second to simulate a live sensor feed.
- Faust is the Python equivalent of Kafka Streams. Its `@app.agent` pattern defines a streaming topology with stateful processing support.
- The `-200` sentinel value in the UCI dataset (indicating missing sensor data) is filtered out during training.
