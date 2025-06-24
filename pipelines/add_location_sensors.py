import argparse
import logging
import os
from dotenv import load_dotenv
from etl.ingestion.fetch_location_sensors import fetch_location_sensors
from etl.transform.transform_location_sensors import transform_location_sensors
from etl.load.load_location_sensors import load_location_sensors
from db.db import get_db

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def add_location_sensors(location_id: int):

    db = next(get_db())

    try:
        logging.info(f"Fetching sensor data for location {location_id}")
        raw_filepath = fetch_location_sensors(location_id)

        clean_filepath = transform_location_sensors(raw_filepath)

        load_location_sensors(clean_filepath, db)

        logging.info(f"Successfully added sensor data for location {location_id}")

    except Exception:
        logging.exception(f"Error while adding sensor data for location {location_id}")
    finally:
        db.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--location", required=True, help="Location id as recognized by the OpenAQ API")
    args = parser.parse_args()

    add_location_sensors(args.location)

if __name__ == "__main__":
    main()
