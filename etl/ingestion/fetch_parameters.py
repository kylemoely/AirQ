import json
from pathlib import Path
import logging
from dotenv import load_dotenv
from datetime import datetime
import requests
import os

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

API_BASE_URL = os.getenv("OPENAQ_API_BASE")
API_KEY = os.getenv("API_KEY")
RAW_DATA_DIR = Path("/home/ec2-user/data/raw")

def fetch_parameters():
    """
    Calls the /parameters endpoint of the OpenAQ API to get information on the API's parameters. Saves raw json data.
    """
    headers = {
            "X-API-KEY": API_KEY
            }

    try:
        logging.info(f"Fetching parameter data.")
        response = requests.get(f"{API_BASE_URL}/parameters", headers=headers)
        response.raise_for_status()
        data = response.json()

        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
        filename = RAW_DATA_DIR / f"parameters_{timestamp}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logging.info(f"Saved parameter data to {filename}.")

    except requests.RequestException as e:
        logging.error(f"Error fetching data from OpenAQ: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def main():
    fetch_parameters()

if __name__ == "__main__":
    main()
