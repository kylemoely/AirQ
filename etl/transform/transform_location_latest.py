import json
import pandas as pd
from pathlib import Path
import argparse
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

DATA_DIR = Path(os.getenv("DATA_DIR"))
RAW_DATA_DIR = DATA_DIR / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DATA_DIR = DATA_DIR / "clean"
CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)

def transform_location_latest(filename: Path) -> Path:
    """
    Transforms raw json file containing latest sensor measurements into parquet format.

    Args:
        filename (Path): Path object pointing to the filename of the raw data json file.

    Raises:
        ValueError: If the file is not .json or the file contains no records in the results array.

    Returns:
        clean_filepath (Path): Path object that points to parquet data file.
    """

    if not filename.name.endswith(".json"):
        raise ValueError(f"Expected json file. Got {filename}")

    filename = filename.name
    filepath = RAW_DATA_DIR / filename
    clean_filepath = CLEAN_DATA_DIR / filename.replace(".json",".parquet")
    with open(filepath, "r") as f:
        data = json.load(f)

    if len(data.get('results', []))==0:
        raise ValueError(f"No records present in {filepath}")

    logging.info(f"Successfully loaded {filename}")

    records = [
            {
                "datetime":r["datetime"]["utc"],
                "api_sensorid":r["sensorsId"],
                "value":r["value"]
            } for r in data["results"]]

    df = pd.DataFrame(records)
    df.to_parquet(clean_filepath ,engine="pyarrow", index=False)
    logging.info(f"Saved {len(records)} clean records to {clean_filepath}.")

    return clean_filepath

def main():
    parser = argparse.ArgumentParser(description="Transform raw location data to parquet.")
    parser.add_argument("--filename", required=True, help="Filename of json file containing data to be transformed.")
    args = parser.parse_args()

    filepath = Path(args.filename)

    transform_location_latest(filepath)

if __name__ == "__main__":
    main()
