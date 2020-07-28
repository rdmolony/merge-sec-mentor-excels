import datetime

import prefect
from prefect import Flow

from secs.extract import (
    get_mentor_excel_filepaths,
    read_excel_to_dict,
    regroup_excels_by_sheet,
)
from secs.transform import transform_sec_activity_by_month_sheet
from secs._filepaths import DATA_DIR, MENTOR_DIR

TODAY = datetime.datetime.today()
ONE_MONTH_AGO = TODAY - datetime.timedelta(days=30)

MASTER_EXCEL = RESULTS_DIR / f"master-{TODAY.strftime('%d-%m-%Y')}.xlsx"


def etl() -> Flow:

    with Flow("Extract, Transform & Load Mentor Excels") as flow:

        mentor_filepaths = get_mentor_excel_filepaths(MENTOR_DIR)
        mentor_excels = read_excel_to_dict.map(mentor_filepaths)
        mentor_excels_by_sheet = regroup_excels_by_sheet(mentor_excels)

        sec_activity_by_month = transform_sec_activity_by_month_sheet(
            mentor_excels_by_sheet["SEC activity by month"]
        )

