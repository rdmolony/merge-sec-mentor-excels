from pathlib import Path

import pandas as pd
import pytest

from secs.extract import get_mentor_excel_filepaths

INPUT_DIR = Path(__file__).parent / "input_data"
MENTOR_DIR = INPUT_DIR / "mentors"


def test_get_mentor_excel_filepaths() -> None:

    output = get_mentor_excel_filepaths.run(MENTOR_DIR)
    output_stems = [filepath.stem for filepath in output]
    expected_output = ["SEC - CM - DCC", "SEC - CM - DLR"]

    assert output_stems == expected_output

