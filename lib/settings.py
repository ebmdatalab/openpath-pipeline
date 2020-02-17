"""Variables used in data processing
"""
from datetime import date
from pathlib import Path
import logging
import os

from dateutils import relativedelta
from pandas.api.types import CategoricalDtype

LOG_LEVEL = logging.getLevelName(os.environ.get("OPATH_LOG_LEVEL", "WARNING"))


# In the spreadsheet that Helen currently manually maintains, indicate
# which columns provide old-test-code-to-new mappings for each lab.
# The spreadsheet is here:
# https://docs.google.com/spreadsheets/d/e/2PACX-1vSeLPEW4rTy_hCktuAXEsXtivcdREDuU7jKfXlvJ7CTEBycrxWyunBWdLgGe7Pm1A/pub?gid=241568377&single=true&output=csv
TEST_CODE_MAPPINGS = {
    "nd": ["nd_testcode"],
    "cornwall": ["cornwall_testcode"],
    "plymouth": ["plym_testcode", "other_plym_codes"],
    "cambridge": [],
}


# XXX not sure of the purpose of this
RANGE_CEILING = 99999

# Combine all values under this value into a single range
SUPPRESS_UNDER = 6
SUPPRESS_STRING = "1-{}".format(SUPPRESS_UNDER - 1)


# Error codes
WITHIN_RANGE = 0
UNDER_RANGE = -1
OVER_RANGE = 1
ERR_NO_REF_RANGE = 2
ERR_UNPARSEABLE_RESULT = 3
ERR_INVALID_SEX = 4
ERR_INVALID_RANGE_WITH_DIRECTION = 5
ERR_DISCARDED_AGE = 6
ERR_INVALID_REF_RANGE = 7

# Friendly names for error codes
ERROR_CODE_NAMES = {
    WITHIN_RANGE: "Within range",
    UNDER_RANGE: "Under range",
    OVER_RANGE: "Over range",
    ERR_NO_REF_RANGE: "No ref range",
    ERR_UNPARSEABLE_RESULT: "Non-numeric result",
    ERR_INVALID_SEX: "Unknown sex",
    ERR_INVALID_RANGE_WITH_DIRECTION: "Insufficient data",
    ERR_DISCARDED_AGE: "Underage for ref range",
    ERR_INVALID_REF_RANGE: "Invalid ref range",
}

# Never process dates older than this date
DATE_FLOOR = date.today() - relativedelta(years=5)

# The keys that every anonymiser_config must export
REQUIRED_NORMALISED_KEYS = ["month", "test_code", "practice_id", "result_category"]

# Working directory for intermediate (i.e. month-by-month) files. Once
# these have been combined successfully, files here are removed,
# except the master all-tests file
INTERMEDIATE_DIR = Path.cwd() / "intermediate_data"

# Directory for data that can be copied out of the secure environment
FINAL_DIR = Path.cwd() / "final_data"

ENV = os.environ.get("OPATH_ENV", "")


def _date_dtype():
    # Build categorical values for months
    month = DATE_FLOOR
    month_categories = []
    while month <= date.today():
        month_categories.append(month.strftime("%Y/%m/01"))
        month += relativedelta(months=1)
    return CategoricalDtype(categories=month_categories, ordered=False)


categorical = CategoricalDtype(ordered=False)


def _result_dtype():
    return CategoricalDtype(
        categories=[
            WITHIN_RANGE,
            UNDER_RANGE,
            OVER_RANGE,
            ERR_NO_REF_RANGE,
            ERR_UNPARSEABLE_RESULT,
            ERR_INVALID_SEX,
            ERR_INVALID_RANGE_WITH_DIRECTION,
            ERR_DISCARDED_AGE,
            ERR_INVALID_REF_RANGE,
        ],
        ordered=False,
    )


INTERMEDIATE_OUTPUT_DTYPES = {
    "month": _date_dtype(),
    "test_code": str,
    "practice_id": str,
    "result_category": _result_dtype(),
}


FINAL_OUTPUT_DTYPES = {
    "ccg_id": categorical,
    "practice_id": categorical,
    "count": int,
    "error": int,
    "lab_id": categorical,
    "practice_name": categorical,
    "result_category": _result_dtype(),
    "test_code": categorical,
    "total_list_size": int,
}
