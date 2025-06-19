import argparse
import json
import pandas as pd
from pathlib import Path
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

DATA_DIR = Path(os.getenv("DATA_DIR"))
RAW_DATA_DIR = DATA_DIR / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DATA_DIR = DATA_DIR / "clean"
CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)

def transform_parameters(filename: Path) -> Path:
    """
    Transforms raw json data containing parameter information to parquet format."
    
    Args:
        filename (Path): Path object that points to the filename of the raw data json file.

    Raises:
        ValueError: If the file is not .json, the file does not contain the word 'parameters', or there are no records in the results array of the json.

    Returns:
        clean_filepath (Path): Path object that points to clean parquet file.
    """

    if not filename.name.endswith(".json"):
        raise ValueError(f"Expected json file. Got {filename}")
    if not "parameters" in filename.name:
        raise ValueError(f"Expected filename to contain 'parameters'. Got {filename}")

    filename = filename.name
    filepath = RAW_DATA_DIR / filename
    clean_filepath = CLEAN_DATA_DIR / filename.replace(".json", ".parquet")

    with open(filepath, "r") as f:
        data = json.load(f)

    if len(data.get('results', []))==0:
        raise ValueError(f"No records present in {filename}")

    logging.info(f"Successfully loaded {filename}")

    records = [
            {
                "api_parameterid": r["id"],
                "units": r["units"],
                "name": r["displayName"],
                "description": r["description"]
                } for r in data["results"]
            ]
    df = pd.DataFrame(records)
    df.to_parquet(clean_filepath, engine="pyarrow", index=False)
    logging.info(f"Saved {len(records)} clean records to {clean_filepath}")

    return clean_filepath

def main():
    parser = argparse.ArgumentParser(description="Transform raw parameter data into parquet.")
    parser.add_argument("--filename", required=True, help="Filename of json file containing parameter data.")
    args = parser.parse_args()
    
    filepath = Path(args.filename)

    transform_parameters(filepath)

if __name__ == "__main__":
    main()
