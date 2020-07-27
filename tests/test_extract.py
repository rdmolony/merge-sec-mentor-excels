from pathlib import Path

import pandas as pd
import pytest

from secs.extract import get_mentor_excel_filepaths, group_mentor_excels_by_sheet

INPUT_DIR = Path(__file__).parent / "input_data"
MENTOR_DIR = INPUT_DIR / "mentors"


def test_get_mentor_excel_filepaths() -> None:

    output = get_mentor_excel_filepaths.run(MENTOR_DIR)
    output_stems = [filepath.stem for filepath in output]
    expected_output = ["SEC - CM - DCC", "SEC - CM - DLR"]

    assert output_stems == expected_output


def test_group_mentor_excels_by_sheet(ref) -> None:

    local_authority_excels_filepaths = MENTOR_DIR / "DCC" / "SEC - CM - DCC.xlsx"
    local_authority_excel = pd.read_excel(
        local_authority_excels_filepaths, sheet_name=None
    )
    local_authority_excels = [local_authority_excel, local_authority_excel]

    output = group_mentor_excels_by_sheet.run(local_authority_excels)
    ref.assertDataFrameCorrect(output, "GroupMentorExcelsBySheet.csv")
