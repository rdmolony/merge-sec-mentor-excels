from collections import defaultdict
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


def _find_sec_name_cell(sheet: Worksheet) -> Cell:

    return [
        cell for column in sheet.columns for cell in column if cell.value == "SEC Name"
    ][0]


def _extract_local_authority_name_from_filepath(filepath: Path) -> str:

    regex = re.compile(r"SEC - CM - (\w+)")
    return regex.findall(filepath.stem)[0]


@task
def _save_sec_activity_by_month_to_excel(
    merged_df: pd.DataFrame, filepath: Path
) -> pd.DataFrame:

    append_df_to_excel(
        filepath, merged_df, sheet_name="SEC activity by month", startrow=7
    )


def append_df_to_excel(
    filename,
    df,
    sheet_name="Sheet1",
    startrow=0,
    truncate_sheet=False,
    **to_excel_kwargs,
):
    """Append a DataFrame [df] to existing Excel file [filename]
    into [sheet_name] Sheet.
    If [filename] doesn't exist, then this function will create it.

    Parameters:
      filename : File path or existing ExcelWriter
                 (Example: '/path/to/file.xlsx')
      df : dataframe to save to workbook
      sheet_name : Name of sheet which will contain DataFrame.
                   (default: 'Sheet1')
      startrow : upper left cell row to dump data frame.
                 Per default (startrow=None) calculate the last row
                 in the existing DF and write to the next row...
      truncate_sheet : truncate (remove and recreate) [sheet_name]
                       before writing DataFrame to Excel file
      to_excel_kwargs : arguments which will be passed to `DataFrame.to_excel()`
                        [can be dictionary]

    Returns: None

    Source:  https://stackoverflow.com/questions/20219254/how-to-write-to-an-existing-excel-file-without-overwriting-data-using-pandas
    """

    # ignore [engine] parameter if it was passed
    if "engine" in to_excel_kwargs:
        to_excel_kwargs.pop("engine")

    writer = pd.ExcelWriter(filename, engine="openpyxl")

    try:
        writer.book = load_workbook(filename)

    except FileNotFoundError:
        writer.book = Workbook()

    # get the last row in the existing Excel sheet
    # if it was not specified explicitly
    if startrow is None and sheet_name in writer.book.sheetnames:
        startrow = writer.book[sheet_name].max_row

    # truncate sheet
    if truncate_sheet and sheet_name in writer.book.sheetnames:
        # index of [sheet_name] sheet
        idx = writer.book.sheetnames.index(sheet_name)
        # remove [sheet_name]
        writer.book.remove(writer.book.worksheets[idx])
        # create an empty sheet [sheet_name] using old index
        writer.book.create_sheet(sheet_name, idx)

        # copy existing sheets
        writer.sheets = {ws.title: ws for ws in writer.book.worksheets}

    # write out the new sheet
    df.to_excel(writer, sheet_name, startrow=startrow, **to_excel_kwargs)

    # save the workbook
    writer.save()
