from pathlib import Path
import logging
import argparse
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

CLEAN_DATA_DIR = Path(os.getenv("DATA_DIR")) / "clean"
CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

def load_location(filename: str):
    """
    Loads clean parquet data into db locations table."

    Args:
        filename (str): Filename of the clean parquet file to be written to database.

    Raises:
        ValueError: If filename is not .parquet, no records are present in the file, or 'location' is not in the filename.
    """

    if not filename.endswith(".parquet"):
        raise ValueError(f"Expected .parquet file. Got {filename}")
    if not 'location' in filename:
        raise ValueError(f"Expected filename to contain 'location'. Got {filename}")

    filename = Path(filename).name
    filepath = CLEAN_DATA_DIR / filename

    df = pd.read_parquet(filepath)
    
    if len(df)==0 or list(df.columns)!=["api_locationid","name","latitude","longitude","api_countryid"]:
        raise ValueError("Improper dataframe in parquet file.")

    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    try:
        logging.info(f"Loading {len(df)} records into locations table.")
        df.to_sql("locations", engine, if_exists="append", index=False)
        logging.info("Load complete.")
    except SQLAlchemyError as e:
        logging.error(f"Error writing to database: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while writing to locations table: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", required=True, help="Filename of the parquet file containing location information to be written to database.")
    args = parser.parse_args()

    load_location(args.filename)

if __name__ == "__main__":
    main()
