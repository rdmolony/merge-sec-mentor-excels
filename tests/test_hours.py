from datetime import datetime
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from secs.tasks.hours import (
    _calculate_sec_activities_total,
    _calculate_other_activities_total,
    _extract_other_activity_hours_for_month,
    _extract_sec_activity_hours_for_month,
    _split_other_activity_hours_for_month,
    _split_sec_activity_hours_for_month,
    calculate_monthly_other_activity_days,
    calculate_monthly_sec_activity_days,
)
from secs.tasks.utilities import get_datetime_from_abbreviated_month

INPUT_DIR = Path(__file__).parent / "input_data"
REFERENCE_DIR = Path(__file__).parent / "reference_data"
MENTOR_DIR = INPUT_DIR / "mentors"


@pytest.fixture
def master_excel() -> Dict[str, pd.DataFrame]:

    master_filepath = REFERENCE_DIR / "master.xlsx"
    return pd.read_excel(master_filepath, sheet_name=None)


def test_extract_sec_activity_hours_for_month(master_excel, ref) -> None:

    month = datetime.strptime(f"Mar 2020", "%b %Y")
    output = _extract_sec_activity_hours_for_month(
        master_excel["SEC activity by month"], month
    )

    # ref.assertDataFrameCorrect(output, filepath)


def test_split_sec_activity_hours_for_month(master_excel, ref) -> None:

    month = datetime.strptime(f"Mar 2020", "%b %Y")
    monthly_hours = _extract_sec_activity_hours_for_month(
        master_excel["SEC activity by month"], month
    )
    _split_sec_activity_hours_for_month(monthly_hours)

    # ref.assertDataFrameCorrect(output, filepath)


def test_calculate_monthly_sec_activity_days(master_excel, ref) -> None:

    month = datetime.strptime(f"Mar 2020", "%b %Y")

    calculate_monthly_sec_activity_days.run(
        master_excel["SEC activity by month"], month
    )


def test_extract_other_activity_hours_for_month(master_excel, ref) -> None:

    month = datetime.strptime(f"Mar 2020", "%b %Y")
    _extract_other_activity_hours_for_month(
        master_excel["Other activity by month"], month
    )


def test_split_other_activity_hours_for_month(master_excel, ref) -> None:

    month = datetime.strptime(f"Mar 2020", "%b %Y")
    monthly_hours = _extract_other_activity_hours_for_month(
        master_excel["Other activity by month"], month
    )
    _split_other_activity_hours_for_month(monthly_hours)


def test_split_other_activity_hours_for_month(master_excel, ref) -> None:

    month = datetime.strptime(f"Mar 2020", "%b %Y")
    monthly_hours = _extract_other_activity_hours_for_month(
        master_excel["Other activity by month"], month
    )
    planned, achieved = _split_other_activity_hours_for_month(monthly_hours)

    _calculate_other_activities_total(planned)


def test_calculate_monthly_other_activity_days(master_excel, ref) -> None:

    month = datetime.strptime(f"Mar 2020", "%b %Y")

    calculate_monthly_other_activity_days.run(
        master_excel["Other activity by month"], month
    )
