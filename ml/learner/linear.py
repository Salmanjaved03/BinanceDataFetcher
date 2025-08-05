from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import pandas as pd
import numpy as np
import json
import pickle
import base64
from datetime import datetime


def generate_linear_regression_signals(ohlcv_df, lookback=30, forward_period=5):
    """
    Generate signals using linear regression to predict future returns
    :param ohlcv_df: DataFrame with OHLCV data
    :param lookback: Window for feature calculation
    :param forward_period: How many periods ahead to predict
    :return: DataFrame with signals
    """
    df = ohlcv_df.copy()

    # Create features (using past returns and volumes)
    df["past_ret_1"] = df["close"].pct_change(1)
    df["past_ret_3"] = df["close"].pct_change(3)
    df["past_ret_5"] = df["close"].pct_change(5)
    df["volatility"] = df["past_ret_1"].rolling(lookback).std()
    df["volume_change"] = df["volume"].pct_change(3)

    # Create target (future returns)
    df["target"] = df["close"].pct_change(forward_period).shift(-forward_period)

    # Drop NA
    df.dropna(inplace=True)

    # Features and target
    feature_cols = [
        "past_ret_1",
        "past_ret_3",
        "past_ret_5",
        "volatility",
        "volume_change",
    ]
    X = df[feature_cols]
    y = df["target"]

    # Train-test split (80/20)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=False
    )

    # Train model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Predict
    df["predicted_return"] = model.predict(X)

    # Calculate MSE for train and test sets
    train_mse = mean_squared_error(y_train, model.predict(X_train))
    test_mse = mean_squared_error(y_test, model.predict(X_test))

    # Generate signals (-1, 0, 1)
    df["signal"] = np.where(
        df["predicted_return"] > 0.005,
        1,
        np.where(df["predicted_return"] < -0.005, -1, 0),
    )

    # Return signals and metadata
    metadata = {
        "model": model,
        "feature_cols": feature_cols,
        "training_rows": len(df),
        "lookback": lookback,
        "forward_period": forward_period,
        "train_mse": train_mse,
        "test_mse": test_mse,
    }

    return df[["datetime", "signal"]], metadata


def save_metadata_to_json(metadata, output_file="signals_metadata.json"):
    """
    Save model metadata to a JSON file.
    :param metadata: Dictionary containing model metadata
    :param output_file: Path to the output JSON file
    """
    try:
        # Serialize model to base64
        model_bytes = pickle.dumps(metadata["model"])
        model_base64 = base64.b64encode(model_bytes).decode("utf-8")

        # Prepare data for JSON
        json_data = {
            "model_data": model_base64,
            "feature_cols": metadata["feature_cols"],
            "training_timestamp": datetime.now().isoformat(),
            "training_rows": metadata["training_rows"],
            "lookback": metadata["lookback"],
            "forward_period": metadata["forward_period"],
            "train_mse": metadata["train_mse"],
            "test_mse": metadata["test_mse"],
        }

        # Write to JSON file
        with open(output_file, "w") as f:
            json.dump(json_data, f, indent=4)
        print(f"Metadata successfully saved to {output_file}")

    except Exception as e:
        print(f"Error saving metadata to JSON: {e}")


def main():
    # Sample OHLCV data (replace with actual data source)
    ohlcv_df = pd.DataFrame(
        {
            "datetime": pd.date_range("2025-01-01", periods=1000, freq="h"),
            "open": np.random.randn(1000).cumsum() + 100,
            "high": np.random.randn(1000).cumsum() + 101,
            "low": np.random.randn(1000).cumsum() + 99,
            "close": np.random.randn(1000).cumsum() + 100,
            "volume": np.random.randint(1000, 10000, 1000),
        }
    )

    # Generate signals and metadata
    signals_df, metadata = generate_linear_regression_signals(
        ohlcv_df, lookback=30, forward_period=5
    )

    # Save metadata to JSON
    save_metadata_to_json(metadata, output_file="signals_metadata.json")

    # Optionally, save signals to a CSV for further use
    signals_df.to_csv("signals_output.csv", index=False)
    print("Signals saved to signals_output.csv")


if __name__ == "__main__":
    main()
