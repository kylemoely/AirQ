import argparse
import os
from dotenv import load_dotenv
from db.db import get_db
from etl.ingestion.fetch_location import fetch_location
from etl.transform.transform_location import transform_location
from etl.load.load_location import load_location
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def add_new_location(location_id: int):

    db = next(get_db())

    try:
        logging.info(f"Fetching data for location {location_id}")
        raw_filepath = fetch_location(location_id)
    except Exception as e:
        logging.exception(f"Error while fetching data from OpenAQ API")

    try:
        clean_filepath = transform_location(raw_filepath)
    except Exception as e:
        logging.exception(f"Error while transforming data from {raw_filepath}")

    try:
        load_location(clean_filepath, db)
        logging.info(f"Successfully added location {location_id} to database.")
    except Exception as e:
        logging.exception(f"Error while insert data into database for {location_id} from {clean_filepath}")

    db.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--location", required=True, help="Location id as recognized by the OpenAQ API")
    args = parser.parse_args()

    add_new_location(args.location)

if __name__ == "__main__":
    main()

