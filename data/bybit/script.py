import time
import subprocess
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


def run_fetcher():
    try:
        print("⏳ Running BybitDataFetcher...")
        result = subprocess.run(
            ["python", r"data/bybit/data_fetcher.py"],  # <-- Change this
            check=True,
            capture_output=True,
            text=True,
        )
        print("✅ Done:\n", result.stdout)
    except subprocess.CalledProcessError as e:
        print("❌ Error running fetcher script:\n", e.stderr)


if __name__ == "__main__":
    while True:
        run_fetcher()
        print("⏲ Waiting 5 minutes...\n")
        time.sleep(300)  # Wait for 5 minutes
