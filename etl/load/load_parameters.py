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

def load_parameters(filename: Path, db: Session):
    """
    Loads clean parquet parameter data into PostgreSQL parameters table.

    Args:
        filename (Path): Path object that points to the file name of the clean parquet data.
        db (Session): SQLAlchemy session object.

    Raises:
        ValueError: If file is not .parquet, parquet file is empty or contains improper column names, or 'parameters' is not in the filename.
    """

    if not filename.name.endswith(".parquet"):
        raise ValueError(f"Expected parquet file. Got {filename}")
    if not "parameters" in filename.name:
        raise ValueError(f"Expected filename to contain 'parameters'. Got {filename}")

    filename = filename.name
    filepath = CLEAN_DATA_DIR / filename

    df = pd.read_parquet(filepath)

    if len(df)==0 or list(df.columns)!= ["id", "units", "name", "description"]:
        raise ValueError(f"Improper dataframe from {filename}")
    
    engine = db.get_bind()
    
    try:
        logging.info(f"Loading {len(df)} records into parameters.")
        df.to_sql("parameters", engine, if_exists="append", index=False)
        logging.info("Load complete.")
    except SQLAlchemyError as e:
        logging.error(f"Error writing to database: {e}")
    except Exception as e:
        logging.error(f"Unexpected error writing to sensors table: {e}")

def main():
    parser = argparse.ArgumentParser(description="Load clean parquet data into airq database.")
    parser.add_argument("--filename", required=True, help="Clean parquet filename to load into parameters table.")
    args = parser.parse_args()

    filepath = Path(args.filename)
    
    db = next(get_db())
    
    try:
        load_parameters(filepath, db)
    finally:
        db.close()

if __name__ == "__main__":
    main()
