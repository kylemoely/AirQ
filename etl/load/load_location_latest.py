import pandas as pd
from pathlib import Path
import argparse
import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

ROOT_DIR = Path("/home/ec2-user/AirQ")
CLEAN_DATA_DIR = ROOT_DIR / "../data/clean"
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def load_location_latest(filename: str):
    """
    Loads clean parquet data into PostgreSQL measurements table.

    Args:
        filename (str): The filename of the clean parquet data.
    """

    if not filename.endswith(".parquet"):
        raise ValueError(f"Expected parquet file. Got {filename}")

    # Check if absolute file path was passed and fix accordingly
    filepath = Path(filename)
    if filepath.is_absolute() or filepath.parts[:len(CLEAN_DATA_DIR.parts)] == CLEAN_DATA_DIR.parts:
        filepath = filepath.relative_to(CLEAN_DATA_DIR)

    filepath = CLEAN_DATA_DIR / filepath

    df = pd.read_parquet(filepath)

    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    logging.info(f"Loading {len(df)} records into measurements.")
    df.to_sql("measurements", engine, if_exists="append", index=False)
    logging.info("Load complete.")


def main():
    parser = argparse.ArgumentParser(description="Load clean parquet data into PostgreSQL.")
    parser.add_argument("--filename", required=True, help="Clean parquet filename to load into measurements table.")
    args = parser.parse_args()

    load_location_latest(args.filename)

if __name__ == "__main__":
    main()
