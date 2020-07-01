from collections import defaultdict
from datetime import datetime
from pathlib import Path
import re
from time import strptime
from typing import List, Union, Dict
from copy import copy

from icontract import require, ensure
import numpy as np
from openpyxl import load_workbook, Workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.cell.cell import Cell
import pandas as pd
from prefect import task


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


def _find_sec_name_cell(sheet: Worksheet) -> Cell:

    return [
        cell for column in sheet.columns for cell in column if cell.value == "SEC Name"
    ][0]


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
    sec_name_col = _find_sec_name_cell(sheet).column - 1

    usecols = [sec_name_col] + [*range(monthly_mentor_col, monthly_mentor_col + 10)]

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
