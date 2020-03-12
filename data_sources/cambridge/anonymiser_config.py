import glob
import os
import csv
import re
from datetime import datetime

from lib.intermediate_file_processing import StopProcessing
from lib.logger import log_warning, log_info

LAB_CODE = "cambridge"
REFERENCE_RANGES = ""

files_path = os.path.join(
    os.environ.get("DATA_BASEDIR", "/home/filr/"), "Cambridge/*.csv"
)
INPUT_FILES = glob.glob(files_path)

RANGE_CEILING = 99999


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
ERR_NO_TEST_CODE = 8


def row_iterator(filename):
    """Provide a way to iterate over every row as a dict in the given file
    """
    with open(filename, "r") as f:
        for row in csv.DictReader(f):
            yield row


def drop_unwanted_data(row):
    """Drop any rows of test data, obviously corrupted data, or otherwise
        unusable data (e.g. no information about the patient's age or the
        practice)
        """
    if not row["CollectedDateTime"]:
        log_warning(row, "Empty date")
        raise StopProcessing()
    if not row["Patient Age"] or row["Patient Age"] < "18":
        raise StopProcessing()


PRACTICE_REGEX = re.compile(r".*\(([A-Z][0-9]{5})[0-9]*\).*")


def normalise_data(row):
    """Convert test results to float wherever possible; extract a
    direction if required; format the date to
    %Y/%m/01.

    Additionally, rename the fields to the standardised list.

    """
    result = row["TestResultValue"]
    try:
        order_date = datetime.strptime(row["CollectedDateTime"], "%d/%m/%Y")
    except ValueError:
        log_warning(row, "Unparseable date %s", result)
        raise StopProcessing()

    row["month"] = order_date.strftime("%Y/%m/01")
    direction = None
    row["dob"] = ""
    row["age"] = ""
    row["sex"] = ""
    try:
        if result.startswith("<"):
            direction = "<"
            result = float(result[1:]) - 0.0000001
        elif result.startswith(">"):
            direction = ">"
            result = float(result[1:]) + 0.0000001
        else:
            result = float(result)
    except ValueError:
        pass
    row["test_result"] = result
    row["direction"] = direction
    # Should probably use regex but this is faster XXX
    practice_code_match = PRACTICE_REGEX.match(row["SubmitterName"])
    if not practice_code_match:
        log_warning(row, "Unparseable practice %s", row["SubmitterName"])
        raise StopProcessing()

    row["requestor_organisation_code"] = practice_code_match.groups()[0]
    col_mapping = {
        "month": "month",
        "test_code": "TestResultName",  # XXX or name...
        "test_result": "test_result",
        "practice_id": "requestor_organisation_code",
        "age": "age",
        "sex": "sex",
        "direction": "direction",
        "result_category": "TestResult",
    }
    mapped = {}
    for k, v in col_mapping.items():
        mapped[k] = row[v]
    return mapped


def convert_to_result(row, ranges):
    """Set a value of the `result_category` key, based on existing fields:

    month, test_code, practice_id, age, sex, direction
    """
    result = row["test_result"]
    return_code = None
    if not isinstance(result, float):
        log_info(row, "Unparseable result")
        row["result_category"] = ERR_UNPARSEABLE_RESULT
        return

    # ['', 'Normal', 'High Critical', 'Abnormal', 'High', 'Low', 'Low
    # Critical'] "Abnormal" appears to be reserved for tests with a
    # non-numeric result, which we mark as "unparseable" here.
    if row["result_category"] in ["High", "High Critical"]:
        return_code = OVER_RANGE
    elif row["result_category"] in ["Low", "Low Critical"]:
        return_code = UNDER_RANGE
    elif row["result_category"] == "Normal":
        return_code = WITHIN_RANGE
    elif row["result_category"] == "":
        return_code = ERR_NO_REF_RANGE  # I think....
    row["result_category"] = return_code
    return row
