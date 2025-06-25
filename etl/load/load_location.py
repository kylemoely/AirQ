from pathlib import Path
import logging
import argparse
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import os
from dotenv import load_dotenv
from db.db import get_db
from sqlalchemy.orm import Session

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

CLEAN_DATA_DIR = Path(os.getenv("DATA_DIR")) / "clean"

def load_location(filename: Path, db: Session):
    """
    Loads clean parquet data into db locations table."

    Args:
        filename (Path): Path object that points to filename of the clean parquet file to be written to database.
        db (Session): SQLAlchemy database session.

    Raises:
        ValueError: If filename is not .parquet, no records are present in the file, or 'location' is not in the filename.
    """

    if not filename.name.endswith(".parquet"):
        raise ValueError(f"Expected .parquet file. Got {filename}")
    if not 'location' in filename.name:
        raise ValueError(f"Expected filename to contain 'location'. Got {filename}")

    filename = filename.name
    filepath = CLEAN_DATA_DIR / filename

    df = pd.read_parquet(filepath)
    
    if len(df)==0 or list(df.columns)!=["id","name","latitude","longitude","country_id"]:
        raise ValueError("Improper dataframe in parquet file.")
    location_id = int(df.loc[0].id)
    engine = db.get_bind()

    try:
        logging.info(f"Loading {len(df)} records into locations table.")
        df.to_sql("locations", engine, if_exists="append", index=False)
        logging.info("Succesfully added location {location_id} to database.")
    except SQLAlchemyError as e:
        logging.error(f"Error writing to database: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while writing to locations table: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", required=True, help="Filename of the parquet file containing location information to be written to database.")
    args = parser.parse_args()
    
    filepath = Path(args.filename)
    db = next(get_db())
    try:
        load_location(filepath, db)
    finally:
        db.close()

if __name__ == "__main__":
    main()
