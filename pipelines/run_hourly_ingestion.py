import logging
from pathlib import Path
from datetime import datetime
import os
import pandas as pd
from sqlalchemy.orm import Session
from db.database import SessionLocal
from db import models
from etl.fetch_location_latest import fetch_location_latest
from etl.transform_location_latest import transform_location_latest
from etl.load_location_latest import load_location_latest
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

DATA_DIR = Path(os.getenv("DATA_DIR"))
CLEAN_DATA_DIR
