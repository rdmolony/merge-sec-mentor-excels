import pandas as pd
from prefect import Flow, task

from _filepaths import DATA_DIR, MENTOR_EXCEL_FILEPATHS
from _globals import MENTOR_LAS


# ETL Flow
# ********

def _process_mentor_excels() -> Flow:

    with Flow("Process SEC mentor excels") as flow:

        # _load_excel_sheet.map(MENTOR_EXCEL_FILEPATHS)

# Tasks
# *****

def _load_excel_sheet(filepath: Path, sheet_name: str) -> pd.DataFrame:

    return pd.read_excel(
        filepath, sheet_name=sheet_name,
    )


# def _extract_hourly_overview_SDCC() -> pd.DataFrame:


