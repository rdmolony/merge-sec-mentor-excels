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

