from prefect import Flow, unmapped, Parameter
from pipeop import pipes

from _filepaths import DATA_DIR, MENTOR_DIR
from _globals import MENTOR_LAS, MENTORS_BY_LA
from utilities.flow import run_flow
from tasks import (
    _get_excel_filepaths,
    _get_excel_filepath,
    _copy_guidance_sheet,
    _load_sec_activity_by_month_sheets_inferring_headers,
    _replace_question_marks_with_nan,
    _drop_empty_rows,
    _concatenate_data_from_multiple_sheets,
    _save_sec_activity_by_month_to_excel,
)


# ETL Flow
# ********

RESULTS_DIR = DATA_DIR / "results"
TODAYS_MASTER_EXCEL = (
    DATA_DIR / "results" / f"master-{datetime.today().strftime('%d-%m-%y')}.xlsx"
)
TEMPLATE_MASTER_EXCEL = DATA_DIR / "master_template.xlsx"


@pipes
def recreate_master_excel_flow() -> Flow:

    with Flow("Recreate Master Excel") as flow:

        filepaths = _get_excel_filepaths(MENTOR_DIR)
        filepath = _get_excel_filepath(MENTOR_DIR, 1)

        _copy_guidance_sheet(TEMPLATE_MASTER_EXCEL, TODAYS_MASTER_EXCEL)

        sec_by_month_sheet_data = (
            _load_sec_activity_by_month_sheets_inferring_headers.map(filepaths)
            >> _replace_question_marks_with_nan.map
            >> _drop_empty_rows.map
            >> _concatenate_data_from_multiple_sheets
        )
        _save_sec_activity_by_month_to_excel(
            sec_by_month_sheet_data, TODAYS_MASTER_EXCEL
        )

    return flow


# @pipes
# def monthly_report_flow() -> Flow:

#     with Flow("Compile monthly SEC mentor report") as flow:

#         filepaths = _get_excel_filepaths(MENTOR_DIR)

#         file_number = Parameter("file_number")
#         filepath = _get_excel_filepath(MENTOR_DIR, file_number)

#         # hourly_overview = (
#         #     _load_sec_activity_by_month_sheet_inferring_headers.map(filepaths)
#         #     >> _drop_empty_rows.map
#         #     >> _get_rlpdt_totals.map
#         #     >> _concatenate_dataframes
#         # )
#         # _save_dataframe_to_excel(hourly_overview, RESULTS_DIR, "total_sec_hours.xlsx")

#         month = Parameter("month")
#         monthly_hours = (
#             _load_monthly_hours_from_sec_activity_by_month_sheet.map(
#                 filepaths, unmapped(month)
#             )
#             >> _replace_question_mark_with_nan.map()
#             >> _drop_empty_rows.map
#             >> _replace_empty_mentors_with_local_authority.map
#             >> _replace_empty_numeric_cells_with_zeros.map
#             >> _label_monthly_hours_as_planned_and_acheived.map
#             >> _calculate_total_monthly_hours.map
#             >> _concatenate_dataframes
#         )
#         _save_dataframe_to_excel(monthly_hours, RESULTS_DIR, "monthly_hours.xlsx")

#         monthly_hours_by_mentor = _get_total_monthly_hours_by_mentor(monthly_hours)
#         _save_dataframe_to_excel(
#             monthly_hours_by_mentor, RESULTS_DIR, "monthly_hours_by_mentor.xlsx"
#         )

#     return flow
