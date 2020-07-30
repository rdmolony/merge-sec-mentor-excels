from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from tdda.referencetest.checkpandas import default_csv_loader

from secs.tasks.extract import regroup_excels_by_sheet
from secs.tasks.transform import (
    transform_sheet,
    _clean_numeric_columns,
)
from secs.tasks.utilities import (
    get_local_authority_name_from_filepath,
    select_numeric_columns,
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

    local_authorities = ["DCC", "DLR"]
    output = transform_sheet.run(
        mentor_excels_by_sheet[sheet_name],
        header_row=header_row,
        local_authorities=local_authorities,
    )
    # ref.assertDataFrameCorrect(output, filename)
