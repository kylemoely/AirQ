from datetime import datetime
import argparse
import json
from pathlib import Path
import os
from dotenv import load_dotenv
import logging
import requests

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

API_KEY = os.getenv("API_KEY")
API_BASE_URL = os.getenv("OPENAQ_API_BASE")
RAW_DATA_DIR = Path(os.getenv("DATA_DIR")) / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

def fetch_location(location_id: int):
    """
    Calls the /locations/location_id endpoint of the OpenAQ API to get location information.

    Args:
        location_id (int): Location id as recognized by the OpenAQ API.
    """

    headers = {"X-API-KEY": API_KEY}

    try:
        logging.info(f"Fetching location information for location: {location_id}")

        response = requests.get(f"{API_BASE_URL}/locations/{location_id}", headers=headers)
        response.raise_for_status()
        data = response.json()

        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H%MSZ")
        filepath = RAW_DATA_DIR / f"location_{location_id}_{timestamp}.json"

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=True, indent=2)

        logging.info(f"Saved {len(data.get('results', []))} records to {filepath}")
    except requests.RequestException as e:
        logging.error(f"Error fetching data from OpenAQ: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--location", required=True, help="Location id as recognized by the OpenAQ API")
    args = parser.parse_args()

    fetch_location(args.location)

if __name__ == "__main__":
    main()
