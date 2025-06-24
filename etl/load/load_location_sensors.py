import pandas as pd
from pathlib import Path
import argparse
import logging
import os
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError
from db.db import get_db
from sqlalchemy.orm import Session

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

CLEAN_DATA_DIR = Path(os.getenv("DATA_DIR")) / "clean"

def load_location_sensors(filename: Path, db: Session):
    """
    Loads clean parquet data for a location's sensors into the sensors database table.

    Args:
        filename (Path): Path object that points to the filename of the parquet file containing sensor data.
        db (Session): SQLAlchemy session object connected to airq database.

    Raises:
        ValueError: If filename is not .parquet, parquet file is empty or contains improper columns, or does not include the string 'location_sensors'.
    """

    if not filename.name.endswith(".parquet"):
        raise ValueError(f"Expected .parquet file. Got {filename}")
    if not 'location_sensors' in filename.name:
        raise ValueError(f"Expected filename to contain 'location_sensors'. Got {filename}")

    filename = filename.name
    filepath = CLEAN_DATA_DIR / filename

    df = pd.read_parquet(filepath)
    
    if len(df)==0 or list(df.columns)!=["id","location_id","parameter_id"]:
        raise ValueError(f"Improper dataframe from {filename}")

    engine = db.get_bind()
    
    try:
        logging.info(f"Loading {len(df)} records into sensors table.")
        df.to_sql("sensors", engine, if_exists="append", index=False)
        logging.info("Load complete.")
    except SQLAlchemyError as e:
        logging.error(f"Error writing to database: {e}")
    except Exception as e:
        logging.error(f"Unexpected error writing to sensors table: {e}")

def main():
    parser = argparse.ArgumentParser(description="Load clean parquet data into airq database.")
    parser.add_argument("--filename", required=True, help="Filename of parquet file containing sensor data to be uploaded.")
    args = parser.parse_args()

    filepath = Path(args.filename)
    
    db = next(get_db())
    try:
        load_location_sensors(filepath, db)
    finally:
        db.close()

if __name__ == "__main__":
    main()
