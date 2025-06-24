import logging
import os
from sqlalchemy import select
from etl.ingestion.fetch_location_latest import fetch_location_latest
from etl.transform.transform_location_latest import transform_location_latest
from etl.load.load_location_latest import load_location_latest
from dotenv import load_dotenv
from db.db import get_db
from db import models

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def run_ingestion():
    logging.info("Ingesting hourly location measurements.")

    db = next(get_db())

    try:
        locations = db.scalars(select(models.Location)).all()
        logging.info(f"Found {len(locations)} locations to fetch.")

        for location in locations:
            print(location.id)
            location_id = location.id
            logging.info(f"Processing location {location_id}: {location.name}")

            raw_filepath = fetch_location_latest(location_id)
            try:
                clean_filepath = transform_location_latest(raw_filepath)
            except Exception as e:
                logging.error(f"Error while transforming json data for location {location_id}. Skipping location.")
                continue
            try:
                load_location_latest(clean_filepath, db)
            except Exception as e:
                logging.error(f"Error while inserting into database data for location {location_id}. Skipping location.")
                continue
            logging.info(f"Measurements for location {location_id} successfully updated.")
    except Exception as e:
        logging.exception(f"Unexpected error during hourly ingestion. Discontinuing.")
    finally:
        db.close()

def main():
    run_ingestion()

if __name__ == "__main__":
    main()

