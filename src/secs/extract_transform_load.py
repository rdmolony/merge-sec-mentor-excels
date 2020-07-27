import datetime

import prefect
from prefect import Flow

from secs.extract import get_mentor_excel_filepaths, read_mentor_excel_to_dict
from secs.transform import transform_mentor_excels
from secs._filepaths import DATA_DIR, MENTOR_DIR

TODAY = datetime.datetime.today()
ONE_MONTH_AGO = TODAY - datetime.timedelta(days=30)

MASTER_EXCEL = RESULTS_DIR / f"master-{TODAY.strftime('%d-%m-%Y')}.xlsx"


def etl() -> Flow:

    with Flow("Extract, Transform & Load Mentor Excels") as flow:

        mentor_filepaths = get_mentor_excel_filepaths(MENTOR_DIR)
        mentor_excels_mapped = read_mentor_excel.map(mentor_filepaths)

        mentor_excel_sheets = transform_mentor_excels(mentor_excel_sheets)
