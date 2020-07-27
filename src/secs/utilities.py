from pathlib import Path
from shutil import copyfile
from typing import Dict

import icontract
from prefect import task


@task
@icontract.require(lambda dirpath: (dirpath / "master_template.xlsx").exists())
def create_master_excel(dirpath: Path) -> None:

    template_master_excel = dirpath / "master_template.xlsx"
    master_excel = dirpath / "master.xlsx"
    copyfile(template_master_excel, master_excel)
