from collections import defaultdict
from datetime import datetime
from pathlib import Path
import re
from time import strptime
from typing import List, Union, Dict
from copy import copy
from shutil import copyfile

from icontract import require, ensure
import numpy as np
from openpyxl import load_workbook, Workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.cell.cell import Cell
import pandas as pd
from prefect import task


@task
def load_excel_workbook(filepath: Path) -> Workbook:

    return


@task
def get_sec_activity_days_by_mentor(filepath: Path, month: str) -> pd.DataFrame:

    sheet_name = "SEC activity by month"
    sec_activity_by_month = load_workbook(filepath)[sheet_name]

    month_number = _convert_month_string_to_number(month)
    month_cell = _find_cell_corresponding_to_month(sec_activity_by_month, month_number)

    monthly_mentor_col = month_cell.column - 2
    sec_name_col = _find_cell_by_name(sec_activity_by_month, "SEC Name").column - 1
    local_authority_col = (
        _find_cell_by_name(sec_activity_by_month, "County / LA").column - 1
    )
    usecols = [sec_name_col, local_authority_col] + [
        *range(monthly_mentor_col, monthly_mentor_col + 10)
    ]

    monthly_hours = pd.read_excel(
        filepath,
        sheet_name=sheet_name,
        header=month_cell.row + 1,
        usecols=usecols,
        engine="openpyxl",
    )

    return monthly_hours


def _convert_month_string_to_number(month: str) -> int:

    return strptime(month, "%B").tm_mon


def _find_cell_corresponding_to_month(sheet: Worksheet, month_number: int) -> Cell:

    all_month_cells = [
        cell
        for column in sheet.columns
        for cell in column
        if isinstance(cell.value, datetime)
    ]

    return [cell for cell in all_month_cells if cell.value.month == month_number][0]


def _find_cell_by_name(sheet: Worksheet, cell_name: str) -> Cell:

    return [
        cell for column in sheet.columns for cell in column if cell.value == cell_name
    ][0]


@task
def split_monthly_sec_activity_hours_into_planned_achieved(
    monthly_hours: pd.DataFrame,
) -> List[pd.DataFrame]:

    monthly_hours_indexed = monthly_hours.set_index(["SEC Name", "County / LA"])
    monthly_hours_split = np.split(monthly_hours_indexed, 2, axis=1)

    return [df.reset_index() for df in monthly_hours_split]


@task
def strip_numbers_from_rlpd_columns(monthly_hours: pd.DataFrame,) -> pd.DataFrame:

    return monthly_hours.rename(columns=lambda x: re.sub("[0-9.]", "", x))


@task
def replace_empty_mentors_with_local_authority(
    monthly_hours: pd.DataFrame,
) -> pd.DataFrame:

    monthly_hours.loc[:, "County Mentor"] = monthly_hours["County Mentor"].fillna(
        monthly_hours["County / LA"],
    )

    return monthly_hours


@task
def replace_empty_numeric_cells_with_zeros(
    monthly_hours: pd.DataFrame,
) -> pd.DataFrame:

    numeric_columns = monthly_hours.select_dtypes("number").columns.tolist()
    monthly_hours.loc[:, numeric_columns] = monthly_hours.fillna(0)

    return monthly_hours


@task
def calculate_total_monthly_hours(monthly_hours: pd.DataFrame,) -> pd.DataFrame:

    return monthly_hours.assign(
        Total=monthly_hours[["Recruit", "Learn", "Plan", "Do"]].sum(axis=1)
    )


@task
def get_total_monthly_hours_by_mentor(monthly_hours: pd.DataFrame,) -> pd.DataFrame:

    return monthly_hours.pivot_table(values="Total", index=["County Mentor", "Status"])
