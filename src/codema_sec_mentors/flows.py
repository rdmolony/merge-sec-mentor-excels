from datetime import datetime

from prefect import Flow, unmapped, Parameter
from pipeop import pipes

from _filepaths import DATA_DIR, MENTOR_DIR
from _globals import MENTOR_LAS, MENTORS_BY_LA
from codema_sec_mentors.utilities.flow import run_flow
from codema_sec_mentors.tasks.general import (
    _get_excel_filepaths,
    _get_excel_filepath,
    _replace_question_marks_with_nan,
    _drop_empty_rows_via_column,
    _concatenate_data_from_multiple_sheets,
)
from codema_sec_mentors.tasks.recreate_master_excel import (
    _create_master_excel_from_template,
    find_header_row_and_load_sheet_to_pandas,
    _save_to_master_excel_sheet,
    _extract_summary_columns,
    _extract_sec_contacts_columns,
)

""" Set Reload to Deep Reload for recursive module reloading...
import builtins
from IPython.lib import deepreload
builtins.reload = deepreload.reload
"""

RESULTS_DIR = DATA_DIR / "results"
MASTER_EXCEL = (
    DATA_DIR / "results" / f"master-{datetime.today().strftime('%d-%m-%y')}.xlsx"
)
TEMPLATE_MASTER_EXCEL = DATA_DIR / "master_template.xlsx"


@pipes
def flow_recreate_master_excel() -> Flow:

    with Flow("Recreate Master Excel") as flow:

        filepaths = _get_excel_filepaths(MENTOR_DIR, MENTOR_LAS)

        create_master_excel = _create_master_excel_from_template(
            TEMPLATE_MASTER_EXCEL, MASTER_EXCEL
        )

        # ETL SEC by month sheet
        load_sec_by_month_sheet = find_header_row_and_load_sheet_to_pandas.map(
            filepaths,
            sheet_name=unmapped("SEC activity by month"),
            cell_name_in_header_row=unmapped("SEC Name"),
        )

        clean_sec_by_month = (
            _replace_question_marks_with_nan.map(load_sec_by_month_sheet)
            >> _drop_empty_rows_via_column.map(unmapped("SEC Name"))
            >> _concatenate_data_from_multiple_sheets
        )

        save_sec_by_month = _save_to_master_excel_sheet(
            clean_sec_by_month,
            MASTER_EXCEL,
            sheet_name="SEC activity by month",
            startrow=7,
        ).set_upstream(create_master_excel)

        # ETL Other activity by month sheet
        load_other_activity_by_month_sheet = find_header_row_and_load_sheet_to_pandas.map(
            filepaths,
            sheet_name=unmapped("Other activity by month"),
            cell_name_in_header_row=unmapped("Region / County"),
            upstream_tasks=[load_sec_by_month_sheet],
        )

        clean_other_activity_by_month = (
            _replace_question_marks_with_nan.map(load_other_activity_by_month_sheet)
            >> _drop_empty_rows_via_column.map(unmapped("Region / County"))
            >> _concatenate_data_from_multiple_sheets
        )

        save_other_activity_by_month = _save_to_master_excel_sheet(
            clean_other_activity_by_month,
            MASTER_EXCEL,
            sheet_name="Other activity by month",
            startrow=7,
        ).set_upstream(save_sec_by_month)

        # ETL Summary sheet
        load_summary_sheet = find_header_row_and_load_sheet_to_pandas.map(
            filepaths,
            sheet_name=unmapped("Summary"),
            cell_name_in_header_row=unmapped("SEC Name"),
            upstream_tasks=[load_other_activity_by_month_sheet],
        )

        clean_summary = (
            _replace_question_marks_with_nan.map(load_summary_sheet)
            >> _drop_empty_rows_via_column.map(unmapped("SEC Name"))
            >> _concatenate_data_from_multiple_sheets
            >> _extract_summary_columns
        )

        save_summary = _save_to_master_excel_sheet(
            clean_summary, MASTER_EXCEL, sheet_name="Summary", startrow=4,
        ).set_upstream(save_other_activity_by_month)

        # ETL SEC contacts sheet
        load_sec_contacts_sheet = find_header_row_and_load_sheet_to_pandas.map(
            filepaths,
            sheet_name=unmapped("SEC contacts"),
            cell_name_in_header_row=unmapped("Name of group/SEC"),
            upstream_tasks=[load_summary_sheet],
        )

        clean_sec_contacts = (
            _replace_question_marks_with_nan.map(load_sec_contacts_sheet)
            >> _drop_empty_rows_via_column.map(unmapped("Name of group/SEC"))
            >> _concatenate_data_from_multiple_sheets
            >> _extract_sec_contacts_columns
        )

        save_sec_contacts = _save_to_master_excel_sheet(
            clean_sec_contacts, MASTER_EXCEL, sheet_name="SEC contacts", startrow=4,
        ).set_upstream(save_summary)

    return flow


@pipes
def monthly_report_flow() -> Flow:

    with Flow("Compile monthly SEC mentor report") as flow:

        filepaths = _get_excel_filepaths(MENTOR_DIR)

        file_number = Parameter("file_number")
        filepath = _get_excel_filepath(MENTOR_DIR, file_number)

        # hourly_overview = (
        #     _load_sec_activity_by_month_sheet_inferring_headers.map(filepaths)
        #     >> _drop_empty_rows.map
        #     >> _get_rlpdt_totals.map
        #     >> _concatenate_dataframes
        # )
        # _save_dataframe_to_excel(hourly_overview, RESULTS_DIR, "total_sec_hours.xlsx")

        month = Parameter("month")
        monthly_hours = (
            _load_monthly_hours_from_sec_activity_by_month_sheet.map(
                filepaths, unmapped(month)
            )
            >> _replace_question_mark_with_nan.map()
            >> _drop_empty_rows.map
            >> _replace_empty_mentors_with_local_authority.map
            >> _replace_empty_numeric_cells_with_zeros.map
            >> _label_monthly_hours_as_planned_and_acheived.map
            >> _calculate_total_monthly_hours.map
            >> _concatenate_dataframes
        )
        _save_dataframe_to_excel(monthly_hours, RESULTS_DIR, "monthly_hours.xlsx")

        monthly_hours_by_mentor = _get_total_monthly_hours_by_mentor(monthly_hours)
        _save_dataframe_to_excel(
            monthly_hours_by_mentor, RESULTS_DIR, "monthly_hours_by_mentor.xlsx"
        )

    return flow
