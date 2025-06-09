import pandas as pd
from pathLib import Path
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

CLEAN_DATA_DIR = Path("/home/ec2-user/
