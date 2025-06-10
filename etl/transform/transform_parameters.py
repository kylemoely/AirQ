import argparse
import json
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

RAW_DATA_DIR = Path("/home/ec2-user/data/raw")
CLEAN_DATA_DIR = Path("/home/ec2-user/data/clean")

def transform_parameters(filename: str):
    """
    Transforms raw json data containing parameter information to parquet format."
    
    Args:
        filename (str): The filename of the raw data json file.

    Raises:
        ValueError: If the file is not .json, the file does not contain the word 'parameters', or there are no records in the results array of the json.
    """

    if not filename.endswith(".json"):
        raise ValueError(f"Expected json file. Got {filename}")
    if not filename.startswith("parameters"):
        raise ValueError(f"Expected filename to contain 'parameters'. Got {filename}")

    filename = Path(filename).name
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

def main():
    parser = argparse.ArgumentParser(description="Transform raw parameter data into parquet.")
    parser.add_argument("--filename", required=True, help="Filename of json file containing parameter data.")
    args = parser.parse_args()

    transform_parameters(args.filename)

if __name__ == "__main__":
    main()
