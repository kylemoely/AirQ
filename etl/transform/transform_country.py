import argparse
import pandas as pd
from pathlib import Path
import os
from dotenv import load_dotenv
import logging
import json


load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

DATA_DIR = Path(os.getenv("DATA_DIR"))
RAW_DATA_DIR = DATA_DIR / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DATA_DIR = DATA_DIR / "clean"
CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)

def transform_country(filename: Path) -> Path:
    """
    Transforms the raw json file containing a country's information into parquet format.

    Args:
        filename (Path): Path object that points to the filename of the raw data json file.

    Raises:
        ValueError: If the file is not .json, does not contain the string 'country', or contains no records in the results array.

    Returns:
        clean_filepath (Path): Path object pointing to clean parquet file.
    """

    if not filename.name.endswith(".json"):
        raise ValueError(f"Expected .json file. Got {filename}")
    if "country" not in filename.name:
        raise ValueError(f"Expected filename to contain 'country'. Got {filename}")

    filename = filename.name
    filepath = RAW_DATA_DIR / filename
    clean_filepath = CLEAN_DATA_DIR / filename.replace(".json",".parquet")

    with open(filepath, "r") as f:
        data = json.load(f)

    if len(data.get("results", []))==0:
        raise ValueError(f"No records present in {filename}")

    logging.info(f"Successfully loaded {filename}")

    records = [
            {
                "id": r["id"],
                "name": r["name"]
                } for r in data["results"]
            ]
    df = pd.DataFrame(records)
    df.to_parquet(clean_filepath, engine="pyarrow", index=False)
    logging.info(f"Saved {len(records)} clean records to {clean_filepath.name}")

    return clean_filepath

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", required=True, help="Filename of the raw json file containing the country's information.")
    args = parser.parse_args()

    filepath = Path(args.filename)

    transform_country(filepath)

if __name__ == "__main__":
    main()
