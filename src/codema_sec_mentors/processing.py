from collections import defaultdict
from datetime import datetime
from pathlib import Path
import re
from time import strptime
from typing import List, Union

from icontract import require, ensure
import numpy as np
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.cell.cell import Cell
import pandas as pd
from prefect import Flow, task, unmapped, Parameter
from pipeop import pipes

from _filepaths import DATA_DIR, MENTOR_DIR
from _globals import MENTOR_LAS
from utilities.flow import run_flow


RESULTS_DIR = DATA_DIR / "results"

# ETL Flow
# ********


@pipes
def process_mentor_excels() -> Flow:

    with Flow("Process SEC mentor excels") as flow:

        filepaths = _get_excel_filepaths(MENTOR_DIR)

        file_number = Parameter("file_number")
        filepath = _get_excel_filepath(MENTOR_DIR, file_number)

        # hourly_overview = (
        #     _load_sec_activity_by_month_sheet_inferring_headers.map(filepaths)
        #     >> _drop_empty_rows.map()
        #     >> _get_rlpdt_totals.map()
        #     >> _merge_dataframes.map()
        # )
        # _save_dataframe_to_excel(hourly_overview, RESULTS_DIR, "total_sec_hours.xlsx")

        month = Parameter("month")
        # monthly_hours = (
        #     _load_monthly_hours_from_sec_activity_by_month_sheet.map(
        #         filepaths, unmapped(month)
        #     )
        #     >> _replace_question_mark_with_nan.map()
        #     >> _drop_empty_rows.map()
        #     >> _replace_empty_mentors_with_local_authority.map()
        #     >> _replace_empty_numeric_cells_with_zeros.map()
        #     >> _label_monthly_hours_as_planned_and_acheived.map()
        #     >> _calculate_total_monthly_hours.map()
        #     >> _merge_dataframes.map()
        # )
        monthly_hours = (
            _load_monthly_hours_from_sec_activity_by_month_sheet(
                filepath, unmapped(month)
            )
            >> _replace_question_mark_with_nan()
            >> _drop_empty_rows()
            >> _replace_empty_mentors_with_local_authority()
            >> _replace_empty_numeric_cells_with_zeros()
            >> _label_monthly_hours_as_planned_and_acheived()
            >> _calculate_total_monthly_hours()
        )
        _save_dataframe_to_excel(monthly_hours, RESULTS_DIR, "monthly_hours.xlsx")

        monthly_hours_by_mentor = _get_total_monthly_hours_by_mentor(monthly_hours)
        _save_dataframe_to_excel(
            monthly_hours_by_mentor, RESULTS_DIR, "monthly_hours_by_mentor.xlsx"
        )

    return flow


# Tasks
# *****


@task
def _get_excel_filepaths(directory) -> List[Path]:

    _all_mentor_dir_excel_files = directory.rglob("*.xlsx")
    return list(
        set(
            [
                filepath
                for filepath in _all_mentor_dir_excel_files
                for la in MENTOR_LAS
                if la in filepath.stem
            ]
        )
    )  # Using list(set()) as a hacky way of removing duplicate filepaths...


@task
def _get_excel_filepath(directory: Path, file_number) -> List[Path]:

    _all_mentor_dir_excel_files = directory.rglob("*.xlsx")
    return list(_all_mentor_dir_excel_files)[file_number]


@task
def _load_sec_activity_by_month_sheet_inferring_headers(filepath: Path) -> pd.DataFrame:

    sheet_name = "SEC activity by month"

    # Find row containing column headers...
    header_row_not_found = True
    row_number = 1
    sheet = load_workbook(filepath)[sheet_name]

    while header_row_not_found:

        row = sheet[row_number]
        row_values = [cell.value for cell in row[:10]]
        # search first 10 columns...
        if "SEC Name" in row_values:
            header_row_not_found = False
            break

        # If not found in first 20 rows...
        if row_number == 20:
            raise ValueError(f"No header row found in {filepath}")

        row_number += 1

    # Load excel sheet using headers found above...
    return pd.read_excel(
        filepath, sheet_name=sheet_name, header=row_number - 1, engine="openpyxl"
    )


@task
def _save_dataframe_to_excel(
    merged_df: pd.DataFrame, savedir: Path, filename: str
) -> pd.DataFrame:

    merged_df.to_excel(savedir / filename)


# Hourly Overview
# ---------------


@task
def _drop_empty_rows(excel_df: pd.DataFrame,) -> pd.DataFrame:
    """Drops all empty rows in the second column - which is column 1 before 
    headers are set...  

    Parameters
    ----------
    excel_df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    return excel_df[excel_df.iloc[:, 1].notna()]


@task
def _get_rlpdt_totals(excel_df: pd.DataFrame,) -> pd.DataFrame:

    excel_df = excel_df.set_index("SEC Name")

    rlpdt_totals = ["Recruit", "Learn", "Plan", "Do", "Total"]

    return excel_df[rlpdt_totals]


@task
def _merge_dataframes(excel_dfs: List[pd.DataFrame]) -> pd.DataFrame:

    return pd.concat(excel_dfs)


# SEC hours by mentor
# -------------------


def _convert_month_string_to_number(month: str) -> int:

    return strptime(month, "%B").tm_mon


def _find_cells_corresponding_to_month(sheet: Worksheet, month_number: int) -> Cell:

    all_month_cells = [
        cell
        for column in sheet.columns
        for cell in column
        if isinstance(cell.value, datetime)
    ]

    return [cell for cell in all_month_cells if cell.value.month == month_number][0]


def _find_sec_name_column(sheet: Worksheet) -> int:

    import ipdb

    ipdb.set_trace()

    # all_month_cells = [
    #     cell
    #     for column in sheet.columns
    #     for cell in column
    #     if isinstance(cell.value, datetime)
    # ]


def _extract_local_authority_name_from_filepath(filepath: Path) -> str:

    regex = re.compile(r"SEC - CM - (\w+)")
    return regex.findall(filepath.stem)[0]


@task
def _load_monthly_hours_from_sec_activity_by_month_sheet(
    filepath: Path, month: str
) -> pd.DataFrame:

    sheet_name = "SEC activity by month"
    sheet = load_workbook(filepath)[sheet_name]

    month_number = _convert_month_string_to_number(month)
    month_cell = _find_cells_corresponding_to_month(sheet, month_number)

    header_row = month_cell.row + 1
    monthly_mentor_col = month_cell.column - 2
    sec_name_col = _find_sec_name_column(sheet)

    usecols = [0] + [*range(monthly_mentor_col, monthly_mentor_col + 10)]

    monthly_hours = pd.read_excel(
        filepath,
        sheet_name=sheet_name,
        header=header_row,
        usecols=usecols,
        engine="openpyxl",
    )

    monthly_hours.attrs = {
        "local_authority": _extract_local_authority_name_from_filepath(filepath)
    }

    return monthly_hours


@task
def _drop_empty_rows(monthly_hours: pd.DataFrame) -> pd.DataFrame:

    return monthly_hours.dropna(how="all")


@task
def _replace_question_mark_with_nan(monthly_hours: pd.DataFrame) -> pd.DataFrame:

    return monthly_hours.replace({"?": np.nan})


@task
def _replace_empty_mentors_with_local_authority(
    monthly_hours: pd.DataFrame,
) -> pd.DataFrame:

    local_authority = monthly_hours.attrs["local_authority"]
    mentors = [col for col in monthly_hours.columns if "Mentor" in col]
    monthly_hours.loc[:, mentors] = monthly_hours[mentors].fillna(local_authority)

    return monthly_hours


@task
def _replace_empty_numeric_cells_with_zeros(
    monthly_hours: pd.DataFrame,
) -> pd.DataFrame:

    numeric_columns = monthly_hours.select_dtypes("number").columns.tolist()
    monthly_hours.loc[:, numeric_columns] = monthly_hours.fillna(0)

    return monthly_hours


@task
def _label_monthly_hours_as_planned_and_acheived(
    monthly_hours: pd.DataFrame,
) -> pd.DataFrame:

    monthly_hours = monthly_hours.set_index("SEC Name")

    monthly_hours.columns = pd.MultiIndex.from_product(
        [["Planned", "Achieved"], ["County Mentor", "Recruit", "Learn", "Plan", "Do"]],
        names=["Status", ""],
    )

    return monthly_hours.stack(0)


@task
def _calculate_total_monthly_hours(monthly_hours: pd.DataFrame,) -> pd.DataFrame:

    return monthly_hours.assign(
        Total=monthly_hours[["Recruit", "Learn", "Plan", "Do"]].sum(axis=1)
    )


@task
def _get_total_monthly_hours_by_mentor(monthly_hours: pd.DataFrame,) -> pd.DataFrame:

    return monthly_hours.pivot_table(values="Total", index=["County Mentor", "Status"])
