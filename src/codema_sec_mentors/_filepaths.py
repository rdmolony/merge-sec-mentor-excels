from pathlib import Path
from typing import List

from _globals import MENTOR_LAS

BASE_DIR = Path(__file__).parents[2]


# Directories
# ***********
DATA_DIR = BASE_DIR / "data"
MENTOR_DIR = DATA_DIR / "mentors"


# Lists of Filepaths
# *****************


def _get_mentor_excel_filepaths() -> List[Path]:

    all_mentor_dir_excel_files = list(MENTOR_DIR.rglob("*.xlsx"))
    return list(
        set(
            [
                filepath
                for filepath in all_mentor_dir_excel_files
                for la in MENTOR_LAS
                if la in filepath.stem
            ]
        )
    )  # Using list(set()) as a hacky way of removing duplicate filepaths...


MENTOR_EXCEL_FILEPATHS = _get_mentor_excel_filepaths()
