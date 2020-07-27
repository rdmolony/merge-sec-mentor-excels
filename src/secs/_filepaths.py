from pathlib import Path
from typing import List

from codema_sec_mentors._globals import MENTOR_LAS

BASE_DIR = Path(__file__).parents[2]

DATA_DIR = BASE_DIR / "data"
RESULTS_DIR = DATA_DIR / "results"
MENTOR_DIR = DATA_DIR / "mentors"

TEMPLATE_MASTER_EXCEL = DATA_DIR / "master_template.xlsx"
