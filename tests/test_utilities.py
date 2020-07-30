from pathlib import Path

import pytest

from secs.tasks.utilities import raise_excels_with_invalid_references_in_sheets

INPUT_DIR = Path(__file__).parent / "input_data"
REFERENCE_DIR = Path(__file__).parent / "reference_data"
MENTOR_DIR = INPUT_DIR / "mentors"


def test_raise_excels_with_invalid_references_in_sheets() -> None:

    filepath_excel_with_invalid_cells = (
        MENTOR_DIR / "DCC" / "SEC - CM - DCC invalid references.xlsx"
    )

    with pytest.raises(ValueError):
        raise_excels_with_invalid_references_in_sheets(
            filepath_excel_with_invalid_cells, ["Summary", "SEC contacts"]
        )
