from datetime import datetime
import argparse
import logging
import json
from pathlib import Path
import os
from dotenv import load_dotenv
import requests

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

RAW_DATA_DIR = Path(os.getenv("DATA_DIR")) / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
API_KEY = os.getenv("API_KEY")
API_BASE_URL = os.getenv("OPENAQ_API_BASE")

def fetch_country(country_id: int) -> Path:
    """
    Calls the countries/country_id OpenAQ endpoint for a given country_id and saves the raw json data.

    Args:
        country_id (int): The country id as recognized by the OpenAQ API.

    Returns:
        filepath (Path): Path object pointing to raw json file.
    """
    headers= {"X-API-KEY": API_KEY}

    try:
        logging.info(f"Fetching data for country: {country_id}")

        response = requests.get(f"{API_BASE_URL}/countries/{country_id}", headers=headers)
        response.raise_for_status()
        data = response.json()

        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
        filepath = RAW_DATA_DIR / f"country_{country_id}_{timestamp}.json"

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=True, indent=2)

        logging.info(f"Saved {len(data.get('results', []))} records to {filepath}")

    except requests.RequestException as e:
        logging.error(f"Error fetching data from OpenAQ: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

    return filepath

def main():
    parser = argparse.ArgumentParser(description="Fetch OpenAQ data for a specified country ID.")
    parser.add_argument("--country", required=True, help="Country id as recognized by the OpenAQ API.")
    args = parser.parse_args()

    fetch_country(args.country)

if __name__ == "__main__":
    main()
