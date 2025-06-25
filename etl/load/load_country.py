import argparse
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os
import logging
from sqlalchemy.exc import SQLAlchemyError
from db.db import get_db
from sqlalchemy.orm import Session

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

CLEAN_DATA_DIR = Path(os.getenv("DATA_DIR")) / "clean"

def load_country(filename: Path, db: Session):
    """
    Loads clean parquet data into countries db table.

    Args:
        filename (Path): Path object that points to filename of the clean parquet data.
        db (Session): SQLAlchemy session object connected to airq database.
    
    Raises:
        ValueError: If file is not .parquet, parquet file is empty or contains improper column names, or filename does not contain 'country'.
    """

    if not filename.name.endswith(".parquet"):
        raise ValueError(f"Expected parquet file. Got {filename}")
    if 'country' not in filename.name:
        raise ValueError(f"Expected 'country' to be in filename. Got {filename}")

    filename = filename.name
    filepath = CLEAN_DATA_DIR / filename

    df = pd.read_parquet(filepath)
    
    if len(df)==0 or list(df.columns) != ["id","name"]:
        raise ValueError(f"Improper dataframe from {filename}")
    country_id = int(df.loc[0].id)

    engine = db.get_bind()
    try:
        logging.info(f"Loading {len(df)} records into countries table.")
        df.to_sql("countries", engine, if_exists="append", index=False)
        logging.info("Succesfully loaded data for country {country_id} into database.")
    except SQLAlchemyError as e:
        logging.error(f"Error writing to database: {e}")
    except Exception as e:
        logging.error(f"Unexpected error writing to database: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", required=True, help="Filename of parquet data to be uploaded to database.")
    args = parser.parse_args()

    filepath = Path(args.filename)
    
    db = next(get_db())
    try:
        load_country(filepath, db)
    finally:
        db.close()

if __name__ == "__main__":
    main()
