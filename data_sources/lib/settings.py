"""Variables used in data processing
"""
from pathlib import Path
from dateutils import relativedelta
from datetime import date
import os

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
DATE_FLOOR = (date.today() - relativedelta(years=5)).strftime("%Y/%m/%d")

# The keys that every anonymiser_config must export
REQUIRED_NORMALISED_KEYS = ["month", "test_code", "practice_id", "result_category"]

# Working directory for intermediate (i.e. month-by-month) files. Once
# these have been combined successfully, files here are removed,
# except the master all-tests file
INTERMEDIATE_DIR = Path.cwd() / "intermediate_data"

# Directory for data that can be copied out of the secure environment
FINAL_DIR = Path.cwd() / "final_data"

ENV = os.environ.get("OPATH_ENV", "")
