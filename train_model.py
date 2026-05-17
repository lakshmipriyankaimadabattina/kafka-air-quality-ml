"""
train_model.py — Offline ML Model Training Script
Trains a Random Forest Regressor on the UCI Air Quality dataset
to predict CO concentration (mg/m³).

Run ONCE before starting the pipeline:
    python train_model.py

Outputs (saved to models/):
    co_model.joblib       — trained Random Forest model
    scaler.joblib         — StandardScaler fitted on training data
    feature_names.joblib  — ordered list of feature column names
"""

import os
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error
from sklearn.preprocessing import StandardScaler
import joblib

DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "AirQualityUCI.csv")
MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")

FEATURES = [
    "PT08.S1(CO)", "C6H6(GT)", "PT08.S2(NMHC)", "NOx(GT)",
    "PT08.S3(NOx)", "NO2(GT)", "PT08.S4(NO2)", "PT08.S5(O3)", "T", "RH", "AH"
]
TARGET = "CO(GT)"


def load_data():
    print(f"[Train] Loading dataset: {DATA_FILE}")
    df = pd.read_csv(DATA_FILE, sep=";", decimal=".")
    # Drop rows where target or any feature is missing
    df = df.dropna(subset=FEATURES + [TARGET])
    # Replace -200 sentinel (UCI missing value marker)
    df = df[(df[TARGET] != -200)]
    for f in FEATURES:
        df = df[df[f] != -200]
    print(f"[Train] Clean rows: {len(df)}")
    return df


def train(df):
    X = df[FEATURES]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"[Train] Train: {len(X_train)} rows | Test: {len(X_test)} rows")

    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)

    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=None,
        random_state=42,
        n_jobs=-1,
    )
    print("[Train] Fitting Random Forest (100 trees) ...")
    model.fit(X_train_s, y_train)

    preds = model.predict(X_test_s)
    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    r2 = r2_score(y_test, preds)

    print("\n── Model Performance (Test Set) ──")
    print(f"   MAE  : {mae:.4f} mg/m³")
    print(f"   RMSE : {rmse:.4f} mg/m³")
    print(f"   R²   : {r2:.4f}")
    print()

    return model, scaler, mae, rmse, r2


def save(model, scaler):
    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(model, os.path.join(MODEL_DIR, "co_model.joblib"))
    joblib.dump(scaler, os.path.join(MODEL_DIR, "scaler.joblib"))
    joblib.dump(FEATURES, os.path.join(MODEL_DIR, "feature_names.joblib"))
    print(f"[Train] Saved model artifacts to {MODEL_DIR}/")


if __name__ == "__main__":
    df = load_data()
    model, scaler, mae, rmse, r2 = train(df)
    save(model, scaler)

    # Write metrics for README reference
    with open(os.path.join(MODEL_DIR, "metrics.txt"), "w") as f:
        f.write(f"Algorithm: Random Forest Regressor (100 trees)\n")
        f.write(f"Target: CO concentration (mg/m3)\n")
        f.write(f"MAE={mae:.4f}\nRMSE={rmse:.4f}\nR2={r2:.4f}\n")

    print("[Train] Done. You can now start the pipeline.")
