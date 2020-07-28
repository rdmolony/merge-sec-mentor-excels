from pathlib import Path
from shutil import copyfile
from typing import Dict

import prefect
from prefect import task


@task
def create_master_excel(template_filepath: Path, master_filepath: Path) -> None:

    logger = prefect.context.get("logger")
    if master_filepath.exists():
        logger.info(f"Skip copying as {master_filepath} already exists ...")
    else:
        logger.info(f"Copying {template_filepath} to {master_filepath} ...")
        copyfile(template_master_excel, master_excel)
