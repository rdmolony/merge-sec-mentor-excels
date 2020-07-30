from pathlib import Path
from shutil import copyfile
from time import sleep
from typing import Dict, List

import pandas as pd
import prefect
from prefect import task

from secs.extract import read_excel_to_dict


@task
def create_master_excel(template_filepath: Path, master_filepath: Path) -> None:

    logger = prefect.context.get("logger")
    if master_filepath.exists():
        logger.info(f"Skip copying as {master_filepath} already exists ...")
    else:
        logger.info(f"Copying {template_filepath} to {master_filepath} ...")
        copyfile(template_filepath, master_filepath)


def dataframe_contains_invalid_references(df: pd.DataFrame) -> bool:

    df = df.copy()

    return any(bug in df.values for bug in ["#REF!", "#VALUE!"])


@task
def raise_excels_with_invalid_references_in_sheets(
    excel_filepath: Path, sheet_names: List[str],
) -> None:

    logger = prefect.context.get("logger")
    sheets = read_excel_to_dict.run(excel_filepath)

    for sheet_name in sheet_names:
        if dataframe_contains_invalid_references(sheets[sheet_name]):
            logger.error(
                f"\n\n{sheet_name} in {excel_filepath} contains invalid references!\n\n"
            )
