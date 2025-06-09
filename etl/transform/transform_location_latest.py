import json
import pandas as pd
from pathlib import Path
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

RAW_DATA_DIR = Path("../data/raw")
CLEAN_DATA_DIR = Path("../data/clean")
CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)

def transform_location_latest(filename: str):
    """
    Transforms raw json file containing latest sensor measurements into parquet format.

    Args:
        filename (str): The filename of the raw data json file.

    Raises:
        ValueError: If the file is not .json or the file contains no records in the results array.
    """

    if not filename.endswith(".json"):
        raise ValueError(f"Expected json file. Got {filename}")

    # Check if absolute file path was passed and fix accordingly.
    filepath = Path(filename)
    if filepath.is_absolute() or filepath.parts[:len(RAW_DATA_DIR.parts)] == RAW_DATA_DIR.parts:
        filepath = filepath.relative_to(RAW_DATA_DIR)

    filepath = RAW_DATA_DIR / filepath
    clean_filepath = CLEAN_DATA_DIR / filepath.name.replace(".json",".parquet")
    with open(filepath, "r") as f:
        data = json.load(f)

    if len(data.get('results', []))==0:
        raise ValueError(f"No records present in {filename}")

    logging.info(f"Successfully loaded {filepath.name}")

    records = [
            {
                "datetime":r["datetime"]["utc"],
                "api_sensorid":r["sensorsId"],
                "value":r["value"]
            } for r in data["results"]]

    df = pd.DataFrame(records)
    df.to_parquet(clean_filepath ,engine="pyarrow", index=False)
    logging.info(f"Saved {len(records)} clean records to {clean_filepath}.")

def main():
    parser = argparse.ArgumentParser(description="Transform raw location data to parquet.")
    parser.add_argument("--filename", required=True, help="Filename of json file containing data to be transformed.")
    args = parser.parse_args()

    transform_location_latest(args.filename)

if __name__ == "__main__":
    main()
