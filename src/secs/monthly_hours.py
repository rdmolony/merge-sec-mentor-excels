import os
from datetime import datetime
from pathlib import Path

import prefect
from prefect import Flow, Parameter, case, unmapped
from prefect_toolkit import run_flow

from secs._filepaths import RESULTS_DIR
from secs.tasks.extract import read_excel_to_dict
from secs.tasks.hours import (
    calculate_misc_activity_totals_by_mentor,
    calculate_sec_activity_totals_by_mentor,
    check_if_too_many_planned_misc_hours,
    check_if_too_many_planned_sec_hours,
    get_misc_activity_days_for_month,
    merge_planned_and_achieved_totals,
    get_sec_activity_days_for_month,
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

        sec_activity_sheet = master_excel_sheets["SEC activity by month"]
        misc_activity_sheet = master_excel_sheets["Other activity by month"]

        sec_hours_by_sec = get_sec_activity_days_for_month(
            sec_activity_sheet, month=month,
        )
        sec_hourly_totals_by_mentor = calculate_sec_activity_totals_by_mentor.map(
            sec_hours_by_sec
        )

        misc_hours_by_sec = get_misc_activity_days_for_month(
            misc_activity_sheet, month=month,
        )
        misc_hourly_totals_by_mentor = calculate_misc_activity_totals_by_mentor.map(
            misc_hours_by_sec
        )

        monthly_totals = get_planned_and_achieved_totals(
            sec_hourly_totals_by_mentor, misc_hourly_totals_by_mentor
        )

        save_dict_of_dataframes_to_excel_sheets(monthly_totals, filepath=master_excel)

        check_if_too_many_planned_sec_hours(sec_activity_sheet, sec_hours_by_sec)
        check_if_too_many_planned_misc_hours(misc_activity_sheet, misc_hours_by_sec)

    return flow
