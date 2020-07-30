from datetime import datetime
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from secs.tasks.utilities import get_datetime_from_abbreviated_month
from secs.tasks.hours import (
    _extract_monthly_hours,
    calculate_monthly_other_activity_days,
    calculate_monthly_sec_activity_days,
)

INPUT_DIR = Path(__file__).parent / "input_data"
REFERENCE_DIR = Path(__file__).parent / "reference_data"
MENTOR_DIR = INPUT_DIR / "mentors"


@pytest.fixture
def master_excel() -> Dict[str, pd.DataFrame]:

    master_filepath = REFERENCE_DIR / "master.xlsx"
    return pd.read_excel(master_filepath, sheet_name=None)


def test_extract_monthly_hours(master_excel, ref) -> None:

    month = get_datetime_from_abbreviated_month("Mar")
    output = _extract_monthly_hours(master_excel["SEC activity by month"], month)
    # ref.assertDataFrameCorrect(output, filename)
