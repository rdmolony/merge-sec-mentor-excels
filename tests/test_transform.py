from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from tdda.referencetest.checkpandas import default_csv_loader

from secs.extract import regroup_excels_by_sheet
from secs.transform import (
    transform_sheet,
    _select_numeric_columns,
    _clean_numeric_columns,
)

INPUT_DIR = Path(__file__).parent / "input_data"
REFERENCE_DIR = Path(__file__).parent / "reference_data"
MENTOR_DIR = INPUT_DIR / "mentors"


@pytest.fixture
def mentor_excels_by_sheet() -> Dict[str, pd.DataFrame]:

    mentor_filepath = MENTOR_DIR / "DCC" / "SEC - CM - DCC.xlsx"
    mentor_excel = pd.read_excel(mentor_filepath, sheet_name=None)
    mentor_excels = [mentor_excel, mentor_excel]

    return regroup_excels_by_sheet.run(mentor_excels)


def test_select_numeric_columns() -> List[str]:

    input = pd.DataFrame(
        {
            "mostly_numbers": [",4", "6!", 1],
            "not_number_column": ["SEC blah", "SEC2", "Hi"],
            "string_with_numbers": ["Level 1", "Level 2", "Level 3"],
            "addresses": ["18 Castleview Heath", "Unit 5 District", "Howth, D13HW18"],
            "mostly_empty_with_numbers": [np.nan, np.nan, 1],
            12: [1, 2, 3],
        }
    )
    expected_output = ["mostly_numbers", "mostly_empty_with_numbers", 12]

    output = _select_numeric_columns(input)
    assert output == expected_output


def test_clean_numeric_columns() -> List[str]:

    input = pd.DataFrame(
        {
            "dirty_col": [",4", "6!", " ", 1, "", "None", 2],
            "clean_col": [1, 2, 3, 4, 5, 6, 7],
        }
    )
    expected_output = pd.DataFrame(
        {"dirty_col": [4, 6, 0, 1, 0, 0, 2], "clean_col": [1, 2, 3, 4, 5, 6, 7]},
    ).convert_dtypes()
    output = _clean_numeric_columns(input)

    assert_frame_equal(output, expected_output)


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
