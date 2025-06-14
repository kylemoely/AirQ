import argparse
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv
import os
import logging
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

CLEAN_DATA_DIR = Path(os.getenv("DATA_DIR")) / "clean"
CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def load_country(filename: str):
    """
    Loads clean parquet data into countries db table.

    Args:
        filename (str): Filename of the clean parquet data.
    
    Raises:
        ValueError: If file is not .parquet, parquet file is empty, or filename does not contain 'country'.
    """

    if not filename.endswith(".parquet"):
        raise ValueError(f"Expected parquet file. Got {filename}")
    if 'country' not in filename:
        raise ValueError(f"Expected 'country' to be in filename. Got {filename}")

    filename = Path(filename).name
    filepath = CLEAN_DATA_DIR / filename

    df = pd.read_parquet(filepath)
    
    if len(df)==0 or list(df.columns) != ["api_countryid","name"]:
        raise ValueError(f"Improper dataframe from {filename}")

    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    try:
        logging.info(f"Loading {len(df)} records into countries table.")
        df.to_sql("countries", engine, if_exists="append", index=False)
        logging.info("Load complete.")
    except SQLAlchemyError as e:
        logging.error(f"Error writing to database: {e}")
    except Exception as e:
        logging.error(f"Unexpected error writing to database: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", required=True, help="Filename of parquet data to be uploaded to database.")
    args = parser.parse_args()

    load_country(args.filename)

if __name__ == "__main__":
    main()
