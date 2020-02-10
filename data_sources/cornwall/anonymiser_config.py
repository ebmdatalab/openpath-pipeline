import os
import glob
import zipfile
import tempfile
import csv
import codecs
from datetime import datetime
import re

from lib.anonymise import StopProcessing

LAB_CODE = "cornwall"
REFERENCE_RANGES = "cornwall_ref_ranges.csv"
files_path = os.path.join(
    os.environ.get("DATA_BASEDIR", "/home/filr/"), "Cornwall/*.zip"
)
INPUT_FILES = glob.glob(files_path)


def row_iterator(filename):
    """Provide a way to iterate over every row as a dict in the given file
    """
    zf = zipfile.ZipFile(filename)
    fname = zf.namelist()[0]
    with zf.open(fname, "r") as zipf:
        for row in csv.DictReader(codecs.iterdecode(zipf, "ISO-8859-1")):
            yield row


def drop_unwanted_data(row):
    """Drop any rows of test data, obviously corrupted data, or otherwise
        unusable data (e.g. no information about the patient's age or the
        practice)
        """
    if not row["PatientDOB"]:
        raise StopProcessing()
    if row["SpecialtyCode"] not in ["600", "180"]:
        raise StopProcessing()


def normalise_data(row):
    """Convert test results to float wherever possible; extract a
    direction if required; set age from DOB; format the date to
    %Y/%m/01.

    Additionally, rename the fields to the standardised list.

    """
    # Replace rows containing floats and percentages with just the floats.
    # See https://github.com/ebmdatalab/openpathology/issues/87#issuecomment-512765880
    #
    # A typical cll looks like `0.03 0.5%`
    FLOAT_PERCENT_RX = re.compile(r"([0-9.])+ +[0-9. ]+%")
    result = re.sub(FLOAT_PERCENT_RX, r"\1", row["TestResult"])
    order_date = datetime.strptime(row["TestOrderDate"], "%Y-%m-%d %H:%M:%S")
    row["month"] = order_date.strftime("%Y/%m/01")
    direction = None
    try:
        dob = datetime.strptime(row["PatientDOB"], "%m-%Y")
        row["age"] = (order_date - dob).days / 365
        if row["age"] < 18:
            raise StopProcessing()
    except ValueError:
        # Couldn't parse age. Drop row.
        raise StopProcessing()
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
        "test_code": "TestResultCode",
        "test_result": "test_result",
        "practice_id": "PracticeCode",
        "age": "age",
        "sex": "PatientGender",
        "direction": "direction",
    }
    mapped = {}
    for k, v in col_mapping.items():
        mapped[k] = row[v]
    return mapped
