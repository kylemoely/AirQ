import pandas as pd
from pathlib import Path
import argparse
import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

CLEAN_DATA_DIR = Path(os.getenv("DATA_DIR")) / "clean"
CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def load_location_sensors(filename: Path):
    """
    Loads clean parquet data for a location's sensors into the sensors database table.

    Args:
        filename (Path): Path object that points to the filename of the parquet file containing sensor data.

    Raises:
        ValueError: If filename is not .parquet or does not include the string 'location_sensors'.
    """

    if not filename.name.endswith(".parquet"):
        raise ValueError(f"Expected .parquet file. Got {filename}")
    if not 'location_sensors' in filename.name:
        raise ValueError(f"Expected filename to contain 'location_sensors'. Got {filename}")

    filename = filename.name
    filepath = CLEAN_DATA_DIR / filename

    df = pd.read_parquet(filepath)

    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
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

    load_location_sensors(filepath)

if __name__ == "__main__":
    main()
