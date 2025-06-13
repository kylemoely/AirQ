import pandas as pd
from pathlib import Path
import argparse
import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

CLEAN_DATA_DIR = Path(os.getenv("DATA_DIR")) / "clean"
CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def load_parameters(filename: str):
    """
    Loads clean parquet parameter data into PostgreSQL parameters table.

    Args:
        filename (str): File name of the clean parquet data.

    Raises:
        ValueError: If file is not .parquet or 'parameters' is not in the filename.
    """

    if not filename.endswith(".parquet"):
        raise ValueError(f"Expected parquet file. Got {filename}")
    if not "parameters" in filename:
        raise ValueError(f"Expected filename to contain 'parameters'. Got {filename}")

    filename = Path(filename).name
    filepath = CLEAN_DATA_DIR / filename

    df = pd.read_parquet(filepath)

    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    logging.info(f"Loading {len(df)} records into parameters.")
    df.to_sql("parameters", engine, if_exists="append", index=False)
    logging.info("Load complete.")

def main():
    parser = argparse.ArgumentParser(description="Load clean parquet data into airq database.")
    parser.add_argument("--filename", required=True, help="Clean parquet filename to load into parameters table.")
    args = parser.parse_args()

    load_parameters(args.filename)

if __name__ == "__main__":
    main()
