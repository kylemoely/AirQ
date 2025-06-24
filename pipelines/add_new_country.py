import logging
from pathlib import Path
import os
from etl.ingestion.fetch_country import fetch_country
from etl.transform.transform_country import transform_country
from etl.load.load_country import load_country
from dotenv import load_dotenv
from db.db import get_db
import argparse

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def add_new_country(country_id: int):
    
    db = next(get_db())

    try:
        logging.info(f"Fetching country information for country {country_id}")

        raw_filepath = fetch_country(country_id)
    except Exception as e:
        logging.exception(f"Error while fetching data from OpenAQ API")

    try:
        clean_filepath = transform_country(raw_filepath)
    except Exception as e:
        logging.exception(f"Error while transforming data from {clean_filepath}")

    try:
        load_country(clean_filepath, db)
        logging.info(f"Country {country_id} successfully added to database.")
    except Exception as e:
        logging.exception(f"Error while inserting data into database for country {country_id} from {clean_filepath}")

    db.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", required=True, help="Country id as recognized by the OpenAQ API.")
    args = parser.parse_args()

    add_new_country(args.country)

if __name__ == "__main__":
    main()
