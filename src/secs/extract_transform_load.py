import datetime
import os
from pathlib import Path

import prefect
from pipeop import pipes
from prefect import Flow, Parameter, unmapped, case

from prefect_toolkit import run_flow

from secs._filepaths import DATA_DIR, MENTOR_DIR, RESULTS_DIR, TEMPLATE_MASTER_EXCEL
from secs.tasks.extract import (
    get_mentor_excel_filepaths,
    read_excel_to_dict,
    regroup_excels_by_sheet,
)
from secs.tasks.load import SaveDataFrameToExcelSheet
from secs.tasks.transform import transform_sheet
from secs.tasks.utilities import (
    create_master_excel,
    raise_excels_with_invalid_references_in_sheets,
    get_local_authority_from_filepath,
)

TODAY = datetime.datetime.today()
ONE_MONTH_AGO = TODAY - datetime.timedelta(days=30)

MASTER_EXCEL = RESULTS_DIR / f"master-{TODAY.strftime('%d-%m-%Y')}.xlsx"


save_to_master_excel_sheet = SaveDataFrameToExcelSheet()


def etl() -> Flow:

    os.environ["PREFECT__LOGGING__LEVEL"] = "DEBUG"
    with Flow("Extract, Transform & Load Mentor Excels") as flow:

        template_master_excel: Path = Parameter(
            "template", default=TEMPLATE_MASTER_EXCEL
        )
        master_excel: Path = Parameter("master", default=MASTER_EXCEL)
        mentor_dir: Path = Parameter("mentor_dir", default=MENTOR_DIR)

        create_master_excel(template_master_excel, master_excel)

        mentor_filepaths = get_mentor_excel_filepaths(mentor_dir)

        raise_excels_with_invalid_references_in_sheets.map(
            mentor_filepaths,
            sheet_names=unmapped(
                [
                    "SEC activity by month",
                    "Other activity by month",
                    "Summary",
                    "SEC contacts",
                ]
            ),
        )

        local_authorities = get_local_authority_from_filepath.map(mentor_filepaths)
        mentor_excels = read_excel_to_dict.map(mentor_filepaths)
        mentor_excels_by_sheet = regroup_excels_by_sheet(mentor_excels)

        sec_activities_by_month = transform_sheet(
            mentor_excels_by_sheet["SEC activity by month"],
            header_row=7,
            local_authorities=local_authorities,
        )
        other_activity_by_month = transform_sheet(
            mentor_excels_by_sheet["Other activity by month"],
            header_row=7,
            local_authorities=local_authorities,
        )
        summary = transform_sheet(
            mentor_excels_by_sheet["Summary"],
            header_row=4,
            local_authorities=local_authorities,
        )
        sec_contacts = transform_sheet(
            mentor_excels_by_sheet["SEC contacts"],
            header_row=4,
            local_authorities=local_authorities,
        )

        save_sec_activities = save_to_master_excel_sheet(
            sec_activities_by_month,
            filepath=master_excel,
            sheet_name="SEC activity by month",
            startrow=7,
        )

        save_other_activities = save_to_master_excel_sheet(
            other_activity_by_month,
            filepath=master_excel,
            sheet_name="Other activity by month",
            startrow=7,
            upstream_tasks=[save_sec_activities],
        )

        save_summary = save_to_master_excel_sheet(
            summary,
            filepath=master_excel,
            sheet_name="Summary",
            startrow=4,
            upstream_tasks=[save_other_activities],
        )

        save_sec_contacts = save_to_master_excel_sheet(
            sec_contacts,
            filepath=master_excel,
            sheet_name="SEC contacts",
            startrow=4,
            upstream_tasks=[save_summary],
        )

    return flow
