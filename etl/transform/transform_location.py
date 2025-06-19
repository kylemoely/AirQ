import argparse
from pathlib import Path
import os
from dotenv import load_dotenv
import pandas as pd
import json
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

DATA_DIR = Path(os.getenv("DATA_DIR"))
RAW_DATA_DIR = DATA_DIR / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DATA_DIR = DATA_DIR / "clean"
CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)

def transform_location(filename: Path) -> Path:
    """
    Transforms raw json file containing location information to parquet format.

    Args:
        filename (Path): Path object that points to file of the raw json data to be cleaned.

    Raises:
        ValueError: If filename is not .json, filename does not contain 'location', or results array contains 0 records.

    Returns:
        clean_filepath (Path): Path object that points to saved parquet data.
    """

    if not filename.name.endswith(".json"):
        raise ValueError(f"Expected json file. Got {filename}")
    if 'location' not in filename.name:
        raise ValueError(f"Expected 'location' to be in filename. Got {filename}")

    filename = filename.name
    filepath = RAW_DATA_DIR / filename
    clean_filepath = CLEAN_DATA_DIR / filename.replace(".json", ".parquet")

    with open(filepath, "r") as f:
        data = json.load(f)

    if len(data.get('results', []))==0:
        raise ValueError(f"No records found in {filename}")

    records = [{
        "api_locationid": r["id"],
        "name": r["name"],
        "latitude": r["coordinates"]["latitude"],
        "longitude": r["coordinates"]["longitude"],
        "api_countryid": r["country"]["id"]
        } for r in data["results"]]

    df = pd.DataFrame(records)
    df.to_parquet(clean_filepath, engine="pyarrow", index=False)
    logging.info(f"Saved {len(df)} clean records to {clean_filepath.name}")

    return clean_filepath

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", required=True, help="Filename of the raw json file containing location information.")
    args = parser.parse_args()
    
    filepath = Path(args.filename)

    transform_location(filepath)

if __name__ == "__main__":
    main()
