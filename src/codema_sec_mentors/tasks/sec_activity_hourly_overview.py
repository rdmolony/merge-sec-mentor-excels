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

