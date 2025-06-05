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
RAW_DATA_DIR = Path("../data/raw")
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

def fetch_openaq_data(city: str, limit: int=100):
    params = {
        "city": city,
        "limit": limit,
        "sort": "desc",
        "order_by":"datetime"
    }

    headers = {
        "X-API-KEY": API_KEY
    }

    try:
        logging.info(f"Fetching data for city: {city}")
        response = requests.get(API_BASE_URL, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()

        timestamp = datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H%M%SZ")
        safe_city = city.lower().replace(" ", "_")
        filename = RAW_DATA_DIR / f"{safe_city}_{timestamp}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logging.info(f"Saved {len(data.get('results', []))} records to {filename}")

    except requests.RequestException as e:
        logging.error(f"Error fetching data from OpenAQ: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def main():
    parser = argparse.ArgumentParser(description="Fetch OpenAQ data for a specified city.")
    parser.add_argument("--city", required=True, help="City name as recognized by OpenAQ (e.g., 'Denver')"
    )
    args = parser.parse_args()

    fetch_openaq_data(city=args.city)

if __name__ == "__main__":
    main()