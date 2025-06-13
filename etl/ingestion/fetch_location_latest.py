import argparse
import requests
import os
import json
from datetime import datetime
from pathlib import Path
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

API_BASE_URL = os.getenv("OPENAQ_API_BASE")
API_KEY = os.getenv("API_KEY")
RAW_DATA_DIR = Path(os.getenv("DATA_DIR")) / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

def fetch_location_latest(location_id: int):
    """
    Calls the locations/location_id/latest OpenAQ API endpoint for a given location_id. Saves raw json data.

    Args:
        location_id (int): The location id as recognized by the OpenAQ API.
    """
    headers = {
        "X-API-KEY": API_KEY
    }

    try:
        logging.info(f"Fetching data for location: {location_id}")
        response = requests.get(f"{API_BASE_URL}/locations/{location_id}/latest", headers=headers)
        response.raise_for_status()
        data = response.json()

        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
        filename = RAW_DATA_DIR / f"location_{location_id}_{timestamp}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logging.info(f"Saved {len(data.get('results', []))} records to {filename}")

    except requests.RequestException as e:
        logging.error(f"Error fetching data from OpenAQ: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def main():
    parser = argparse.ArgumentParser(description="Fetch OpenAQ data for a specified city.")
    parser.add_argument("--location", required=True, help="Location id as recognized by the OpenAQ API.")
    args = parser.parse_args()

    fetch_location_latest(location_id=args.location)

if __name__ == "__main__":
    main()
