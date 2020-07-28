import datetime

import prefect
from pipeop import pipes
from prefect import Flow
from prefect.utilities.debug import raise_on_exception

from prefect_toolkit import run_flow

from secs._filepaths import DATA_DIR, MENTOR_DIR, RESULTS_DIR, TEMPLATE_MASTER_EXCEL
from secs.extract import (
    get_mentor_excel_filepaths,
    read_excel_to_dict,
    regroup_excels_by_sheet,
)
from secs.load import save_to_master_excel_sheet
from secs.transform import transform_sheet
from secs.utilities import create_master_excel

TODAY = datetime.datetime.today()
ONE_MONTH_AGO = TODAY - datetime.timedelta(days=30)

MASTER_EXCEL = RESULTS_DIR / f"master-{TODAY.strftime('%d-%m-%Y')}.xlsx"


@pipes
def etl() -> Flow:

    with Flow("Extract, Transform & Load Mentor Excels") as flow:

        create_master_excel(TEMPLATE_MASTER_EXCEL, MASTER_EXCEL)

        mentor_filepaths = get_mentor_excel_filepaths(MENTOR_DIR)
        mentor_excels = read_excel_to_dict.map(mentor_filepaths)
        mentor_excels_by_sheet = regroup_excels_by_sheet(mentor_excels)

        transform_sheet(
            mentor_excels_by_sheet["SEC activity by month"], "SEC activity by month"
        ) >> save_to_master_excel_sheet(
            filepath=MASTER_EXCEL, sheet_name="SEC activity by month", startrow=8
        )

        transform_sheet(
            mentor_excels_by_sheet["Other activity by month"], "Other activity by month"
        ) >> save_to_master_excel_sheet(
            filepath=MASTER_EXCEL, sheet_name="Other activity by month", startrow=8
        )

        transform_sheet(
            mentor_excels_by_sheet["Summary"], "Summary"
        ) >> save_to_master_excel_sheet(
            filepath=MASTER_EXCEL, sheet_name="Summary", startrow=5
        )

        transform_sheet(
            mentor_excels_by_sheet["SEC contacts"], "SEC Contacts"
        ) >> save_to_master_excel_sheet(
            filepath=MASTER_EXCEL, sheet_name="SEC contacts", startrow=5
        )

    return flow
