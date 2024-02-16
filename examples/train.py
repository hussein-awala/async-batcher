"""
This script trains a model on the diabetes dataset and saves it to a file.

The model can be one of the following:
- XGBoost
- Random Forest
- TensorFlow
"""
from __future__ import annotations

import argparse
import pickle

from sklearn.datasets import load_diabetes


def xgb_model(X, y, file_name="diabetes_xgb_model.pkl"):
    from xgboost import XGBRegressor

    model = XGBRegressor(n_estimators=10000, random_state=42)
    model.fit(X, y)
    with open(file_name, "wb") as f:
        pickle.dump(model, f)


def rf_model(X, y, file_name="diabetes_rf_model.pkl"):
    from sklearn.ensemble import RandomForestRegressor

    model = RandomForestRegressor(n_estimators=10000, random_state=42)
    model.fit(X, y)
    with open(file_name, "wb") as f:
        pickle.dump(model, f)


def tf_model(X, y, file_name="diabetes_tf_model.h5"):
    from tensorflow.keras.layers import Dense
    from tensorflow.keras.models import Sequential

    model = Sequential()
    model.add(Dense(units=64, activation="relu", input_shape=(10,)))
    for _ in range(48):
        model.add(Dense(units=64, activation="relu"))
    model.add(Dense(units=1, activation=None))
    model.compile(optimizer="adam", loss="mean_squared_error")

    model.fit(X, y, epochs=100, batch_size=32)

    model.save(file_name)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--model", type=str, required=True)
    args = arg_parser.parse_args()

    diabetes = load_diabetes()

    X = diabetes.data
    y = diabetes.target

    if args.model == "xgb":
        xgb_model(X, y)
    elif args.model == "rf":
        rf_model(X, y)
    elif args.model == "tf":
        tf_model(X, y)
    else:
        raise ValueError("Unknown model")
