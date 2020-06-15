from collections import defaultdict
from datetime import datetime
from pathlib import Path
import re
from typing import List, Union

from icontract import require, ensure
import numpy as np
from openpyxl import load_workbook
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

        # hourly_overview = (
        #     _load_sec_activity_by_month_sheet_inferring_headers.map(filepaths)
        #     >> _drop_empty_rows.map()
        #     >> _get_rlpdt_totals.map()
        #     >> _merge_mentor_dataframes
        # )
        # _save_dataframe_to_excel(hourly_overview, RESULTS_DIR, "total_sec_hours.xlsx")

        monthly_hours_planned_raw, monthly_hours_achieved_raw = (
            _load_monthly_hours_from_sec_activity_by_month_sheet.map(filepaths, "june")
            >> _split_monthly_hours_into_planned_and_acheived.map()
        )

        monthly_hours_planned_clean, monthly_hours_achieved_clean = (
            _replace_question_mark_with_nan.map(
                monthly_hours_planned_raw, monthly_hours_achieved_raw
            )
            >> _drop_empty_rows.map()
            >> _extract_local_authority_name_from_filepath.map(filepaths)
            >> _replace_empty_mentors_with_local_authority
            >> _replace_empty_numeric_cells_with_zeros
        )
        _save_dataframe_to_excel(
            hourly_overview, RESULTS_DIR, "monthly_mentor_hours.xlsx"
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
def _merge_mentor_dataframes(excel_dfs: List[pd.DataFrame]) -> pd.DataFrame:

    return pd.concat(excel_dfs)


# SEC hours by mentor
# -------------------


@task
@require(lambda month: month in ("june", "july"))
def _load_monthly_hours_from_sec_activity_by_month_sheet(
    filepath: Path, month_string: str
) -> pd.DataFrame:

    sheet_name = "SEC activity by month"
    sheet = load_workbook(filepath)[sheet_name]

    months_to_number = {
        "june": 6,
        "july": 7,
    }
    month_number = months_to_number[month_string]

    all_month_cells = [
        cell
        for column in sheet.columns
        for cell in column
        if isinstance(cell.value, datetime)
    ]

    month_cell = [cell for cell in all_month_cells if cell.value.month == month_number][
        0
    ]
    row = month_cell.row + 1
    column = month_cell.column

    header_row = row + 1
    usecols = [0] + [*range(column, column + 8)]

    return pd.read_excel(
        filepath,
        sheet_name=sheet_name,
        header=header_row,
        usecols=usecols,
        engine="openpyxl",
    )


@task
def _split_monthly_hours_into_planned_and_acheived(
    monthly_hours: pd.DataFrame,
) -> Union[pd.DataFrame]:

    sec_names = monthly_hours.iloc[:, 0].to_frame()
    planned = monthly_hours.iloc[:, :5]
    achieved = pd.concat([sec_names, monthly_hours.iloc[:, 5]])

    return planned, achieved


@task
def _replace_question_mark_with_nan(monthly_hours: pd.DataFrame) -> pd.DataFrame:

    return monthly_hours.replace({"?": np.nan})


@task
def _drop_empty_rows(monthly_hours: pd.DataFrame) -> pd.DataFrame:

    return monthly_hours.dropna(how="all")


@task
def _extract_local_authority_name_from_filepath(filepath: Path) -> str:

    regex = re.compile(r"SEC - CM - (\w+)")
    return regex.finall(filepath.stem)[0]


@task
def _replace_empty_mentors_with_local_authority(
    monthly_hours: pd.DataFrame, local_authority: str
) -> pd.DataFrame:

    mentors = monthly_hours.columns[0]
    monthly_hours.loc[:, mentors] = monthly_hours[mentors].fillna(local_authority)

    return monthly_hours


@task
def _replace_empty_numeric_cells_with_zeros(
    monthly_hours: pd.DataFrame,
) -> pd.DataFrame:

    numeric_columns = monthly_hours.select_dtypes("number").columns.tolist()
    monthly_hours.loc[:, numeric_columns] = monthly_hours.fillna(0)

    return monthly_hours
