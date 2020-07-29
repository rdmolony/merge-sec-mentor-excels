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


@pytest.mark.parametrize(
    "sheet_name,header_row,filename",
    [
        ("SEC activity by month", 7, "SecActivityByMonth.csv"),
        ("Other activity by month", 7, "OtherActivityByMonth.csv"),
        ("Summary", 4, "Summary.csv"),
        ("SEC contacts", 4, "SecContacts.csv"),
    ],
)
def test_transform_sheet(
    mentor_excels_by_sheet, sheet_name, header_row, filename
) -> None:

    output = transform_sheet.run(
        mentor_excels_by_sheet[sheet_name], header_row=header_row
    )
    # ref.assertDataFrameCorrect(output, filename)
