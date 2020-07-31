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
    get_planned_and_achieved_totals,
)
from secs.tasks.load import SaveDictDataFramesToExcel

save_dict_of_dataframes_to_excel_sheets = SaveDictDataFramesToExcel()

MASTER_EXCEL = RESULTS_DIR / "master-31-07-2020.xlsx"


def hours() -> Flow:

    master_excel = MASTER_EXCEL
    os.environ["PREFECT__LOGGING__LEVEL"] = "DEBUG"
    month = datetime.strptime(f"Jul 2020", "%b %Y")

    with Flow("Calculate Monthly Fees") as flow:

        master_excel_sheets = read_excel_to_dict(master_excel)

        sec_hours = calculate_monthly_sec_activity_days(
            master_excel_sheets["SEC activity by month"], month=month,
        )

        other_hours = calculate_monthly_other_activity_days(
            master_excel_sheets["Other activity by month"], month=month,
        )

        monthly_totals = get_planned_and_achieved_totals(sec_hours, other_hours)

        save_dict_of_dataframes_to_excel_sheets(monthly_totals, filepath=master_excel)

    return flow
