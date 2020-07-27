import datetime

from prefect import Flow, unmapped, Parameter
from pipeop import pipes

from codema_sec_mentors._filepaths import (
    DATA_DIR,
    RESULTS_DIR,
    MENTOR_DIR,
    TEMPLATE_MASTER_EXCEL,
)
from codema_sec_mentors._globals import MENTOR_LAS, MENTORS_BY_LA
from codema_sec_mentors.utilities.flow import run_flow
from codema_sec_mentors.tasks.general import (
    _get_excel_filepaths,
    _get_excel_filepath,
    _replace_question_marks_with_nan,
    _drop_empty_rows_via_column,
    _concatenate_data_from_multiple_sheets,
)
from codema_sec_mentors.tasks.get_mentor_monthly_totals import (
    load_excel_workbook,
    get_sec_activity_days_by_mentor,
    split_monthly_sec_activity_hours_into_planned_achieved,
    replace_empty_mentors_with_local_authority,
    replace_empty_numeric_cells_with_zeros,
    strip_numbers_from_rlpd_columns,
    calculate_total_monthly_hours,
    get_total_monthly_hours_by_mentor,
)

""" Set Reload to Deep Reload for recursive module reloading...
import builtins
from IPython.lib import deepreload
builtins.reload = deepreload.reload
"""

TODAY = datetime.datetime.today()
ONE_MONTH_AGO = TODAY - datetime.timedelta(days=30)

MASTER_EXCEL = RESULTS_DIR / f"master-{TODAY.strftime('%d-%m-%Y')}.xlsx"


@pipes
def flow_get_monthly_mentor_totals() -> Flow:

    with Flow("Get Mentor Monthly Totals") as flow:

        month = Parameter("month", default=TODAY.strftime("%B"))
        master_monthly_excel = RESULTS_DIR / f"master-{month.run()}.xlsx"

        rlpd_sec_monthly_totals = (
            get_sec_activity_days_by_mentor(MASTER_EXCEL, month.run())
            >> _drop_empty_rows_via_column(unmapped("SEC Name"))
            >> split_monthly_sec_activity_hours_into_planned_achieved
            >> strip_numbers_from_rlpd_columns.map
            >> replace_empty_mentors_with_local_authority.map
            >> replace_empty_numeric_cells_with_zeros.map
            >> calculate_total_monthly_hours.map
        )

        mentor_monthly_totals = get_total_monthly_hours_by_mentor.map(
            rlpd_sec_monthly_totals
        )

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

