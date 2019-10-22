import argparse
import os
import zipfile
import tempfile
import csv
from datetime import datetime
import re
from pathlib import Path

from anonymise import StopProcessing
from anonymise import process_files

cwd = Path(__file__).parent


def row_iterator(filename):
    """Provide a way to iterate over every row as a dict in the given file
    """
    # The default na_values will convert sodium (NA) into NaN!
    zf = zipfile.ZipFile(filename)
    fname = zf.namelist()[0]
    with tempfile.TemporaryDirectory() as d:
        # XXX don't have to extract to disk, we can stream in memory
        zf.extract(fname, path=d)
        with open(os.path.join(d, fname), "r", newline="", encoding="ISO-8859-1") as f:
            for row in csv.DictReader(f):
                yield row


def filter_to_gp_or_a_and_e(row):
    if row["SpecialtyCode"] not in ["600", "180"]:
        raise StopProcessing()


def drop_bad_data(row):
    """Drop any rows of test data, obviously corrupted data, or otherwise
        unusable data (e.g. no information about the patient's age or the
        practice)
        """
    if not row["PatientDOB"]:
        raise StopProcessing()


def normalise_data(self):
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
    result = re.sub(FLOAT_PERCENT_RX, r"\1", self.row["TestResult"])
    order_date = datetime.strptime(self.row["TestOrderDate"], "%Y-%m-%d %H:%M:%S")
    self.row["month"] = order_date.strftime("%Y/%m/01")
    direction = None
    try:
        dob = datetime.strptime(self.row["PatientDOB"], "%m-%Y")
        self.row["age"] = (order_date - dob).days / 365
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
        self.row["test_result"] = result
        self.row["direction"] = direction

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
            mapped[k] = self.row[v]
        self.row = mapped
    except ValueError:
        self.log_info("Unparseable result %s", result)
        raise StopProcessing()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate suitably anonymised subset of raw input data"
    )
    parser.add_argument("lab", help="Lab code (used for naming files)")
    parser.add_argument("files", nargs="+", help="Monthly input files")
    parser.add_argument(
        "--multiprocessing", help="Use multiprocessing", action="store_true"
    )
    args = parser.parse_args()
    filters = (filter_to_gp_or_a_and_e, drop_bad_data)
    normaliser = normalise_data
    process_files(
        args.lab,
        args.files,
        row_iterator,
        filters,
        normalise_data,
        args.multiprocessing,
    )
