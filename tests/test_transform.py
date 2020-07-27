from pathlib import Path

import pandas as pd
import pytest

from secs.transform import transform_mentor_excels

INPUT_DIR = Path(__file__).parent / "input_data"
MENTOR_DIR = INPUT_DIR / "mentors"


def test_transform_mentor_excels(ref):

    mentor_filepath = MENTOR_DIR / "DCC" / "SEC - CM - DCC.xlsx"
    mentor_excel_sheets = pd.read_excel(mentor_filepath, sheet_name=None)

    output = transform_mentor_excels.run(mentor_excel_sheets)

