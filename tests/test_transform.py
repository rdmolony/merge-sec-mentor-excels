from pathlib import Path

import pandas as pd
import pytest

from secs.extract import regroup_excels_by_sheet
from secs.transform import transform_sec_activity_by_month_sheet

INPUT_DIR = Path(__file__).parent / "input_data"
MENTOR_DIR = INPUT_DIR / "mentors"


def test_transform_sec_activity_by_month_sheet(ref):

    mentor_filepath = MENTOR_DIR / "DCC" / "SEC - CM - DCC.xlsx"
    mentor_excel = pd.read_excel(mentor_filepath, sheet_name=None)
    mentor_excels = [mentor_excel, mentor_excel]
    mentor_excels_by_sheet = regroup_excels_by_sheet.run(mentor_excels)

    output = transform_sec_activity_by_month_sheet.run(
        mentor_excels_by_sheet["SEC activity by month"]
    )
    ref.assertDataFrameCorrect(output, "SecActivityByMonth.csv")


# def test_transform_other_activity_by_month_sheet(ref):

#     mentor_filepath = MENTOR_DIR / "DCC" / "SEC - CM - DCC.xlsx"
#     mentor_excel = pd.read_excel(mentor_filepath, sheet_name=None)
#     mentor_excels = [mentor_excel, mentor_excel]
#     mentor_excels_by_sheet = regroup_excels_by_sheet.run(mentor_excels)

#     output = transform_other_activity_by_month_sheet.run(
#         mentor_excels_by_sheet["Other activity by month"]
#     )
#     ref.assertDataFrameCorrect(output, "OtherActivityByMonth.csv")
