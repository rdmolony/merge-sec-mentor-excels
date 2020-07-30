from pathlib import Path

import numpy as np
import pytest
import pandas as pd

from secs.extract_transform_load import etl

INPUT_DIR = Path(__file__).parent / "input_data"
REFERENCE_DIR = Path(__file__).parent / "reference_data"
MENTOR_DIR = INPUT_DIR / "mentors"

TEMPLATE_MASTER_EXCEL = INPUT_DIR / "master_template.xlsx"
MASTER_EXCEL = REFERENCE_DIR / "master.xlsx"


def test_etl(ref) -> None:

    flow = etl()
    flow.run(
        parameters=dict(
            template=TEMPLATE_MASTER_EXCEL, master=MASTER_EXCEL, mentor_dir=MENTOR_DIR
        )
    )
