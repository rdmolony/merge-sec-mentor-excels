import datetime
import os
from pathlib import Path

import prefect
from pipeop import pipes
from prefect import Flow, Parameter, unmapped
from prefect.tasks.core.constants import Constant
from prefect_toolkit import run_flow

from secs._filepaths import DATA_DIR, MENTOR_DIR, RESULTS_DIR, TEMPLATE_MASTER_EXCEL
from secs.tasks.extract import (
    get_mentor_excel_filepaths,
    read_excel_to_dict,
    regroup_excels_by_sheet,
)
from secs.tasks.load import SaveDataFramesToExcel, SaveDataFrameToExcelSheet
from secs.tasks.transform import transform_sheet
from secs.tasks.utilities import (
    create_master_excel,
    get_local_authority_from_filepath,
    raise_excels_with_invalid_references_in_sheets,
)

TODAY = datetime.datetime.today()
ONE_MONTH_AGO = TODAY - datetime.timedelta(days=30)

MASTER_EXCEL = RESULTS_DIR / f"master-{TODAY.strftime('%d-%m-%Y')}.xlsx"


save_to_master_excel_sheet = SaveDataFrameToExcelSheet()
save_dataframes_to_excel_sheets = SaveDataFramesToExcel()


def etl() -> Flow:

    os.environ["PREFECT__LOGGING__LEVEL"] = "DEBUG"
    with Flow("Extract, Transform & Load Mentor Excels") as flow:

        template_master_excel: Path = Parameter(
            "template", default=TEMPLATE_MASTER_EXCEL
        )
        master_excel: Path = Parameter(
            "master", default=MASTER_EXCEL,
        )
        mentor_dir: Path = Parameter(
            "mentor_dir", default=MENTOR_DIR,
        )

        create_master_excel(template_master_excel, master_excel)

        sheet_names = Constant(
            (
                "SEC activity by month",
                "Other activity by month",
                "Summary",
                "SEC contacts",
            ),
            name="Master Excel Sheet Names",
        )
        header_rows = Constant((7, 7, 4, 4), name="Header rows for each sheet")

        mentor_filepaths = get_mentor_excel_filepaths(mentor_dir)

        raise_excels_with_invalid_references_in_sheets.map(
            mentor_filepaths, sheet_names=unmapped(sheet_names),
        )

        local_authorities = get_local_authority_from_filepath.map(mentor_filepaths)
        mentor_excels_raw = read_excel_to_dict.map(mentor_filepaths)
        mentor_excels_by_sheet = regroup_excels_by_sheet(mentor_excels_raw)

        mentor_excels_clean = transform_sheet.map(
            unmapped(mentor_excels_by_sheet),
            sheet_names,
            header_row=header_rows,
            local_authorities=unmapped(local_authorities),
        )

        save_dataframes_to_excel_sheets(
            dfs=mentor_excels_clean,
            filepath=master_excel,
            sheet_names=sheet_names,
            header_rows=header_rows,
        )

    return flow
