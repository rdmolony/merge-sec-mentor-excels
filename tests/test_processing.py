from io import StringIO

import pandas as pd
from pandas.testing import assert_frame_equal

from codema_sec_mentors.processing import (
    _load_sec_activity_by_month_sheet_inferring_headers,
)


@pytest.fixture
def mentor_excel(tmp_path) -> Path:

    excel_file = tmp_path / "mentor.xlsx"

    mentor_data = pd.read_csv(
        StringIO(
            """SEC activity by month
            Lot 2
            SEC location and type



            SEC Name, County / LA, SEC Level (at application)
            Blessington Tidy Towns SEC Group, Wicklow, Level 2
            """
        ),
        skipinitialspace=True,
    )

    mentor_data.to_excel(excel_file)

    return excel_file


def test_load_sec_activity_by_month_sheet_inferring_headers(mentor_excel) -> None:

    input = mentor_excel

    expected_output = pd.read_csv(
        StringIO(
            """SEC Name, County / LA, SEC Level (at application)
            Blessington Tidy Towns SEC Group, Wicklow, Level 2
            """
        ),
        skipinitialspace=True,
    )

    output = _load_sec_activity_by_month_sheet_inferring_headers.run(input)
    assert_frame_equal(output, expected_output)
