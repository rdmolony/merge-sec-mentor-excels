from collections import defaultdict
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
def _create_master_excel_from_template(template: Path, master: Path) -> None:

    copyfile(template, master)


# @task
# def _copy_guidance_sheet(template: Path, todays_master: Path):

#     template_workbook = load_workbook(template)

#     try:
#         todays_workbook = load_workbook(todays_master)

#     except FileNotFoundError:
#         todays_workbook = Workbook()

#     guidance_sheet = todays_workbook.create_sheet("Guidance")

#     for row in template_workbook["Guidance"]:
#         for cell in row:
#             guidance_sheet[cell.coordinate] = cell.value

#         if cell.has_style:
#             guidance_sheet[cell.coordinate].style = cell.style
#             # guidance_sheet[cell.coordinate].font = copy(cell.font)
#             # guidance_sheet[cell.coordinate].border = copy(cell.border)
#             # guidance_sheet[cell.coordinate].fill = copy(cell.fill)
#             # guidance_sheet[cell.coordinate].number_format = copy(cell.number_format)
#             # guidance_sheet[cell.coordinate].protection = copy(cell.protection)
#             # guidance_sheet[cell.coordinate].alignment = copy(cell.alignment)

#     todays_workbook.save(todays_master)


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
def _save_sec_activity_by_month_to_master_excel_openpyxl(
    merged_df: pd.DataFrame, filepath: Path
) -> pd.DataFrame:

    # https://stackoverflow.com/questions/20219254/how-to-write-to-an-existing-excel-file-without-overwriting-data-using-pandas
    book = load_workbook(filepath)
    writer = pd.ExcelWriter(filepath, engine="openpyxl")
    writer.book = book
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

    merged_df.to_excel(
        writer, sheet_name="SEC activity by month", startrow=6, index=False,
    )
    writer.save()


@task
def _save_sec_activity_by_month_to_master_excel_xlsxwriter(
    merged_df: pd.DataFrame, filepath: Path
) -> pd.DataFrame:

    # https://stackoverflow.com/questions/20219254/how-to-write-to-an-existing-excel-file-without-overwriting-data-using-pandas
    book = load_workbook(filepath)
    writer = pd.ExcelWriter(filepath, engine="xlsxwriter")
    writer.book = book
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

    # Turn off the default header and skip one row to allow us to insert a
    # user defined header.
    merged_df.to_excel(
        writer,
        sheet_name="SEC activity by month",
        startrow=7,
        index=False,
        header=False,
        engine="xlsxwriter",
    )

    _set_custom_header(
        merged_df.columns.values, writer, sheet_name="SEC activity by month", row=6
    )
    writer.save()


def _set_custom_header(
    header_names: List[str], writer: pd.ExcelWriter, sheet_name: str, row: int
) -> None:

    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    header_format = workbook.add_format(
        {"bold": True, "text_wrap": True, "valign": "top"}
    )

    # Write the column headers with the defined format.
    for col_num, value in enumerate(header_names):
        worksheet.write(row, col_num, value, header_format)
