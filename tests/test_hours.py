from datetime import datetime
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from secs.tasks.utilities import get_datetime_from_abbreviated_month
from secs.tasks.hours import (
    _extract_sec_activity_hours_for_month,
    _split_sec_activity_hours_for_month,
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


def test_extract_sec_activity_hours_for_month(master_excel, ref) -> None:

    month = get_datetime_from_abbreviated_month("Mar")
    output = _extract_sec_activity_hours_for_month(
        master_excel["SEC activity by month"], month
    )

    filepath = REFERENCE_DIR / "ExtractSecHoursMar.csv"
    ref.assertDataFrameCorrect(output, filepath)


def test_split_sec_activity_hours_for_month(ref) -> None:

    filepath = REFERENCE_DIR / "ExtractSecHoursMar.csv"
    activities_for_month = pd.read_csv(filepath)

    planned, achieved = _split_sec_activity_hours_for_month(activities_for_month)

    ref.assertDataFrameCorrect(output, filepath)
