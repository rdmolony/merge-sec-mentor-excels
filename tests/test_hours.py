from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

INPUT_DIR = Path(__file__).parent / "input_data"
REFERENCE_DIR = Path(__file__).parent / "reference_data"
MENTOR_DIR = INPUT_DIR / "mentors"


@pytest.fixture
def master_excel() -> Dict[str, pd.DataFrame]:

    master_filepath = REFERENCE_DIR / "master.xlsx"
    return pd.read_excel(mentor_filepath, sheet_name=None)


# def test_calculate_monthly_sec_activity_days() -> None:
