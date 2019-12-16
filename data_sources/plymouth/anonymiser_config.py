import glob
import os
import zipfile
import csv
import codecs
from datetime import datetime

from lib.anonymise import StopProcessing

LAB_CODE = "plymouth"
REFERENCE_RANGES = ""

files_path = os.path.join(
    os.environ.get("DATA_BASEDIR", "/home/filr/"), "Plymouth/*.zip"
)
INPUT_FILES = glob.glob(files_path)
assert INPUT_FILES, "No input files found at {}".format(files_path)

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
    zf = zipfile.ZipFile(filename)
    for fname in zf.namelist():
        with zf.open(fname, "r") as zipf:
            for row in csv.DictReader(codecs.iterdecode(zipf, "ISO-8859-1")):
                yield row


def drop_unwanted_data(row_anonymiser):
    """Drop any rows of test data, obviously corrupted data, or otherwise
        unusable data (e.g. no information about the patient's age or the
        practice)
        """
    if not row_anonymiser.row["specimen_taken_date"]:
        row_anonymiser.log_warning("Empty date")
        raise StopProcessing()
    if row_anonymiser.row["patient_age"] < "18":
        raise StopProcessing()


def normalise_data(row_anonymiser):
    """Convert test results to float wherever possible; extract a
    direction if required; set age from DOB; format the date to
    %Y/%m/01.

    Additionally, rename the fields to the standardised list.

    """
    row = row_anonymiser.row
    result = row_anonymiser.row["analyte_result_measurement"]
    try:
        order_date = datetime.strptime(
            row_anonymiser.row["specimen_taken_date"], "%Y-%m-%d"
        )
    except ValueError:
        row_anonymiser.log_warning("Unparseable date %s", result)
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

    col_mapping = {
        "month": "month",
        "test_code": "analyte_lab_code",
        "test_result": "test_result",
        "practice_id": "requestor_organisation_code",
        "age": "age",
        "sex": "sex",
        "direction": "direction",
        "Reference Range": "Reference Range",
    }
    mapped = {}
    for k, v in col_mapping.items():
        mapped[k] = row[v]
    row_anonymiser.row = mapped


def convert_to_result(self):
    """Set a value of the `result_category` key, based on existing fields:

    month, test_code, practice_id, age, sex, direction
    """
    result = self.row["test_result"]
    direction = self.row["direction"]
    ref_range = self.row["Reference Range"]
    if not ref_range:
        self.row["result_category"] = ERR_NO_REF_RANGE
        return
    return_code = None
    try:
        low, high = [float(x) for x in ref_range.split("{")]
    except ValueError:
        self.row["result_category"] = ERR_INVALID_REF_RANGE
        return
    if not isinstance(result, float):
        self.log_info("Unparseable result")
        self.row["result_category"] = ERR_UNPARSEABLE_RESULT
        return
    if high:
        if result > high:
            if direction == "<":
                self.log_warning("Over range %s but result <; invalid", high)
                return_code = ERR_INVALID_RANGE_WITH_DIRECTION
            else:
                self.log_info("Over range %s", high)
                return_code = OVER_RANGE
        elif result < low:
            if direction == ">":
                self.log_warning("Under range %s but >; invalid", high)
                return_code = ERR_INVALID_RANGE_WITH_DIRECTION
            else:
                self.log_info("Under range %s", low)
                return_code = UNDER_RANGE
        else:
            if not direction or (
                (direction == "<" and low == 0)
                or (direction == ">" and high == RANGE_CEILING)
            ):
                self.log_info("Within range %s - %s", low, high)
                return_code = WITHIN_RANGE
            else:
                self.log_warning(
                    "Within range %s-%s but direction %s; invalid", low, high, direction
                )
                return_code = ERR_INVALID_RANGE_WITH_DIRECTION

    else:
        return_code = ERR_INVALID_REF_RANGE
        self.log_warning("Couldn't process ref range %s - %s", low, high)
    self.row["result_category"] = return_code
