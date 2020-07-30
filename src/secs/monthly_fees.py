import os

import prefect
from prefect import Flow, Parameter, unmapped, case

from prefect_toolkit import run_flow

from secs.tasks.extract import read_excel_to_dict
from secs.tasks.load import SaveDataFrameToExcelSheet
from secs.tasks.hours import (
    calculate_monthly_sec_activity_days,
    calculate_monthly_other_activity_days,
)

save_to_master_excel_sheet = SaveDataFrameToExcelSheet()


def calculate_monthly_fees(master_excel: Path) -> Flow:

    os.environ["PREFECT__LOGGING__LEVEL"] = "DEBUG"
    with Flow("Calculate Monthly Fees") as flow:

        master_excel_sheets = read_excel_to_dict(master_excel)

        calculate_monthly_sec_activity_days(
            master_excel_sheets["SEC activity by month"]
        )

        calculate_monthly_other_activity_days(
            master_excel_sheets["Other activity by month"]
        )

    return flow
