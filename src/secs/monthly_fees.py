from datetime import datetime
import os
from pathlib import Path

import prefect
from prefect import Flow, Parameter, case, unmapped
from prefect_toolkit import run_flow

from secs._filepaths import RESULTS_DIR
from secs.tasks.extract import read_excel_to_dict
from secs.tasks.hours import (
    calculate_monthly_other_activity_days,
    calculate_monthly_sec_activity_days,
)
from secs.tasks.load import SaveDataFrameToExcelSheet, SaveDataFramesToExcel

save_to_master_excel_sheet = SaveDataFrameToExcelSheet()
save_dataframes_to_excel_sheets = SaveDataFramesToExcel()

MASTER_EXCEL = RESULTS_DIR / "master-31-07-2020.xlsx"


def fees() -> Flow:

    master_excel = MASTER_EXCEL
    os.environ["PREFECT__LOGGING__LEVEL"] = "DEBUG"
    month = datetime.strptime(f"Jul 2020", "%b %Y")

    with Flow("Calculate Monthly Fees") as flow:

        master_excel_sheets = read_excel_to_dict(master_excel)

        sec_hours = calculate_monthly_sec_activity_days(
            master_excel_sheets["SEC activity by month"], month=month,
        )
        planned_sec = sec_hours["planned"]
        achieved_sec = sec_hours["achieved"]

        other_hours = calculate_monthly_other_activity_days(
            master_excel_sheets["Other activity by month"], month=month,
        )
        planned_other = other_hours["planned"]
        achieved_other = other_hours["achieved"]

    return flow
