import argparse
import requests
import os
import json
from pathlib import Path
import logging
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

API_BASE_URL = os.getenv("OPENAQ_API_BASE")
API_KEY = os.getenv("API_KEY")
RAW_DATA_DIR = Path(os.getenv("DATA_DIR")) / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

def fetch_location_sensors(location_id: int):
    """
    Calls the locations/location_id/sensors OpenAQ API endpoint for a given location_id. Saves the raw json data.

    Args:
        location_id (int): The location id as recognized by the OpenAQ API.
    """
    headers = {
            "X-API-KEY": API_KEY
            }
    try:
        logging.info(f"Fetching sensor data for location: {location_id}")
        response = requests.get(f"{API_BASE_URL}/locations/{location_id}/sensors", headers=headers)
        response.raise_for_status()
        data = response.json()

        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
        filename = RAW_DATA_DIR / f"location_sensors_{location_id}_{timestamp}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logging.info(f"Saved sensor data for location {location_id} to {filename}.")

    except requests.RequestException as e:
        logging.error(f"Error fetching data from OpenAQ: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def main():
    parser = argparse.ArgumentParser(description="Fetch OpenAQ sensor data for a specified location.")
    parser.add_argument("--location", required=True, help="Location id as recognized by the OpenAQ API.")
    args = parser.parse_args()

    fetch_location_sensors(args.location)

if __name__ == "__main__":
    main()
