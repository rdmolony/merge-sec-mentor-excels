from pathlib import Path
from typing import Dict

import pandas as pd
import pytest
from tdda.referencetest.checkpandas import default_csv_loader
from secs.extract import regroup_excels_by_sheet
from secs.transform import transform_sheet

INPUT_DIR = Path(__file__).parent / "input_data"
REFERENCE_DIR = Path(__file__).parent / "reference_data"
MENTOR_DIR = INPUT_DIR / "mentors"


@pytest.fixture
def mentor_excels_by_sheet() -> Dict[str, pd.DataFrame]:

    mentor_filepath = MENTOR_DIR / "DCC" / "SEC - CM - DCC.xlsx"
    mentor_excel = pd.read_excel(mentor_filepath, sheet_name=None)
    mentor_excels = [mentor_excel, mentor_excel]

    return regroup_excels_by_sheet.run(mentor_excels)


def test_transform_sec_activity_by_month_sheet(mentor_excels_by_sheet, ref):

    output = transform_sheet.run(
        mentor_excels_by_sheet["SEC activity by month"], "Unnamed: 1"
    )
    ref.assertDataFrameCorrect(output, "SecActivityByMonth.csv")


def test_transform_other_activity_by_month_sheet(mentor_excels_by_sheet, ref):

    output = transform_sheet.run(
        mentor_excels_by_sheet["Other activity by month"], "Unnamed: 0"
    )
    ref.assertDataFrameCorrect(output, "OtherActivityByMonth.csv")


def test_transform_summary_sheet(mentor_excels_by_sheet, ref):

    output = transform_sheet.run(mentor_excels_by_sheet["Summary"], "Summary")
    ref.assertDataFrameCorrect(output, "Summary.csv")


def test_transform_contacts_sheet(mentor_excels_by_sheet, ref):

    output = transform_sheet.run(mentor_excels_by_sheet["SEC contacts"], "SEC Contacts")
    ref.assertDataFrameCorrect(output, "SecContacts.csv")
