import pandas as pd
from pathlib import Path
import argparse
import logging
import os
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from db.db import get_db

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

CLEAN_DATA_DIR = Path(os.getenv("DATA_DIR")) / "clean"

def load_location_latest(filename: Path, db: Session):
    """
    Loads clean parquet data into PostgreSQL measurements table.

    Args:
        filename (Path): Path object that points to the filename of the clean parquet data.
        db (Session): SQLAlchemy session object

    Raises:
        ValueError: If file is not .parquet, parquet file is empty or contains improper columns, or filename does not contain 'location_latest'. 
    """

    if not filename.name.endswith(".parquet"):
        raise ValueError(f"Expected parquet file. Got {filename}")
    if "location_latest" not in filename.name:
        raise ValueError(f"Expected filename to contain 'location_latest'. Got {filename}")

    filename = filename.name
    filepath = CLEAN_DATA_DIR / filename

    if not filepath.exists():
        raise FileNotFoundError(f"{filepath} does not exist")

    df = pd.read_parquet(filepath)
    
    if len(df)==0 or list(df.columns)!=["datetime", "sensor_id", "value"]:
        raise ValueError(f"Improper dataframe from {filename}")

    engine = db.get_bind()
    
    try:
        logging.info(f"Loading {len(df)} records into measurements.")
        df.to_sql("measurements", engine, if_exists="append", index=False)
        logging.info("Load complete.")
    except SQLAlchemyError as e:
        logging.error(f"Error writing to database: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def main():
    parser = argparse.ArgumentParser(description="Load clean parquet data into PostgreSQL.")
    parser.add_argument("--filename", required=True, help="Clean parquet filename to load into measurements table.")
    args = parser.parse_args()

    filepath = Path(args.filename)
    db = next(get_db())
    try:
        load_location_latest(filepath, db)
    finally:
        db.close()

if __name__ == "__main__":
    main()
