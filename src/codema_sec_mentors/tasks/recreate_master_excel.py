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
def _copy_guidance_sheet(template: Path, todays_master: Path):

    template_workbook = load_workbook(template)

    try:
        todays_workbook = load_workbook(todays_master)

    except FileNotFoundError:
        todays_workbook = Workbook()

    guidance_sheet = todays_workbook.create_sheet("Guidance")

    for row in template_workbook["Guidance"]:
        for cell in row:
            guidance_sheet[cell.coordinate] = cell.value

        if cell.has_style:
            guidance_sheet[cell.coordinate].style = cell.style
            # guidance_sheet[cell.coordinate].font = copy(cell.font)
            # guidance_sheet[cell.coordinate].border = copy(cell.border)
            # guidance_sheet[cell.coordinate].fill = copy(cell.fill)
            # guidance_sheet[cell.coordinate].number_format = copy(cell.number_format)
            # guidance_sheet[cell.coordinate].protection = copy(cell.protection)
            # guidance_sheet[cell.coordinate].alignment = copy(cell.alignment)

    todays_workbook.save(todays_master)


@task
def _load_sec_activity_by_month_sheets_inferring_headers(
    filepath: Path,
) -> pd.DataFrame:

    sheet_name = "SEC activity by month"
    sheet = load_workbook(filepath)[sheet_name]

    # -1 as Pandas is indexed at zero & Excel is indexed at one
    header_row = _find_sec_name_cell(sheet).row - 1

    sec_activity_by_month = pd.read_excel(
        filepath, sheet_name=sheet_name, header=header_row, engine="openpyxl",
    )

    sec_activity_by_month.attrs = {
        "local_authority": _extract_local_authority_name_from_filepath(filepath)
    }

    return sec_activity_by_month


@task
def _save_sec_activity_by_month_to_excel(
    merged_df: pd.DataFrame, filepath: Path
) -> pd.DataFrame:

    append_df_to_excel(
        filepath, merged_df, sheet_name="SEC activity by month", startrow=7
    )
