"""Generates anonymised datasets from input files and JSON-based configurations.

Currently only works for XLS formatted inputs without column headers
"""
from functools import lru_cache
import csv
import json
import logging
import os
import pandas as pd

from . import settings


class StopProcessing(Exception):
    pass


@lru_cache(maxsize=1)
def get_ref_ranges(path):
    """Load a CSV of reference ranges into a list of dicts
    """
    # columns must be ["test", "min_adult_age", "max_adult_age", "low_F", "low_M", "high_F", "high_M"]
    with open(path, newline="", encoding="ISO-8859-1") as f:
        lines = sorted(list(csv.DictReader(f)), key=lambda x: x["test"])
    return lines


# Cache the fact any reference ranges are missing
NO_REF_RANGES = set()


# Core implementations


def skip_old_data(row):
    if row["month"] < settings.DATE_FLOOR:
        raise StopProcessing()


def standard_convert_to_result(row, ranges):
    """Set a value of the `result_category` key in the `row` dict, based
    on existing fields:

    month, test_code, practice_id, age, sex, direction

    """
    test_code = row["test_code"]
    result = row["test_result"]
    sex = row["sex"]
    age = row["age"]
    direction = row["direction"]
    if test_code in NO_REF_RANGES:
        row["result_category"] = settings.ERR_NO_REF_RANGE
        return row
    last_matched_test = None
    found = False
    return_code = None
    for ref_range in ranges:
        if ref_range["test"] == test_code:
            found = True
            if not isinstance(result, float):
                log_info(row, "Unparseable result")
                return_code = settings.ERR_UNPARSEABLE_RESULT
                break
            high = low = None
            if last_matched_test and last_matched_test != ref_range["test"]:
                # We can short-circuit as the rows are sorted by test
                log_info(row, "No matching ref range found")
                return_code = settings.ERR_NO_REF_RANGE
                break
            last_matched_test = ref_range["test"]
            if age >= int(float(ref_range["min_adult_age"])) and age < int(
                float(ref_range["max_adult_age"])
            ):
                # We've found a reference range matching this row's age
                if sex == "M":
                    if ref_range["low_M"] and ref_range["high_M"]:
                        low = float(ref_range["low_M"])
                        high = float(ref_range["high_M"])
                elif sex == "F":
                    if ref_range["low_F"] and ref_range["high_F"]:
                        low = float(ref_range["low_F"])
                        high = float(ref_range["high_F"])
                else:
                    return_code = settings.ERR_INVALID_SEX
                    log_info(row, "Invalid sex %s", sex)
                    break
                if low != "" and high != "" and low is not None and high is not None:
                    if result > high:
                        if direction == "<":
                            log_warning(
                                row, "Over range %s but result <; invalid", high
                            )
                            return_code = settings.ERR_INVALID_RANGE_WITH_DIRECTION
                            break
                        else:
                            log_info(row, "Over range %s", high)
                            return_code = settings.OVER_RANGE
                            break
                    elif result < low:
                        if direction == ">":
                            log_warning(row, "Under range %s but >; invalid", high)
                            return_code = settings.ERR_INVALID_RANGE_WITH_DIRECTION
                            break
                        else:
                            log_info(row, "Under range %s", low)
                            return_code = settings.UNDER_RANGE
                            break
                    else:
                        if not direction or (
                            (direction == "<" and low == 0)
                            or (direction == ">" and high == settings.RANGE_CEILING)
                        ):
                            log_info(row, "Within range %s - %s", low, high)
                            return_code = settings.WITHIN_RANGE
                            break
                        else:
                            log_warning(
                                "Within range %s-%s but direction %s; invalid",
                                low,
                                high,
                                direction,
                            )
                            return_code = settings.ERR_INVALID_RANGE_WITH_DIRECTION
                            break

                else:
                    return_code = settings.ERR_INVALID_REF_RANGE
                    log_warning(row, "Couldn't process ref range %s - %s", low, high)
                    break
            else:
                return_code = settings.ERR_DISCARDED_AGE
    if not found:
        NO_REF_RANGES.add(test_code)
        log_info(row, "Couldn't find ref range")
        return_code = settings.ERR_NO_REF_RANGE
    row["result_category"] = return_code
    return row


# Logging
streamhandler = logging.StreamHandler()
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[streamhandler],
)

logger = logging.getLogger()


def log(row, level, msg, *args):
    msg = msg + " %s "
    args = args + (json.dumps(row),)
    getattr(logger, level)(msg, *args)


def log_warning(row, msg, *args):
    return log(row, "warning", msg, *args)


def log_info(row, msg, *args):
    return log(row, "info", msg, *args)


def log_error(row, msg, *args):
    return log(row, "error", msg, *args)


class Anonymiser:
    def __init__(
        self,
        lab,
        reference_ranges,
        row_iterator=None,
        drop_unwanted_data=None,
        normalise_data=None,
        convert_to_result=None,
    ):
        self.rows = []
        self.lab = lab
        self.row_iterator = row_iterator
        self.drop_unwanted_data = drop_unwanted_data
        self.normalise_data = normalise_data
        self.convert_to_result = convert_to_result or standard_convert_to_result
        if os.path.isfile(reference_ranges):
            self.ref_ranges = get_ref_ranges(reference_ranges)
        else:
            self.ref_ranges = []

    def feed_file(self, filename):
        for i, row in enumerate(self.row_iterator(filename)):
            try:
                self.drop_unwanted_data(row)
                row = self.normalise_data(row)
                skip_old_data(row)
                row = self.convert_to_result(row, self.ref_ranges)
            except StopProcessing:
                row = None

            if row:
                if i == 0:
                    # Check all the required keys have been provided
                    # (in the first row only)
                    provided_keys = set(row.keys())
                    required_keys = set(settings.REQUIRED_NORMALISED_KEYS)
                    missing_keys = required_keys - provided_keys
                    assert not missing_keys, "Required keys missing: {}".format(
                        missing_keys
                    )
                # Only output the columns we care about
                subset = [row[k] for k in settings.REQUIRED_NORMALISED_KEYS]
                self.rows.append(subset)

    def to_csv(self):
        df = pd.DataFrame(columns=settings.REQUIRED_NORMALISED_KEYS, data=self.rows)
        cols = ["month", "test_code", "practice_id", "result_category"]
        df["count"] = 1

        # Make a filename which reasonably represents the contents of
        # the file and doesn't already exist
        date_collected = (
            df.groupby("month").count()["test_code"].sort_values().index[-1]
        )
        converted_basename = "{}converted_{}_{}".format(
            settings.ENV, self.lab, date_collected.replace("/", "_")
        )
        dupes = 0
        if os.path.exists("{}.csv".format(converted_basename)):
            dupes += 1
            candidate_basename = "{}_{}".format(converted_basename, dupes)
            while os.path.exists("{}.csv".format(candidate_basename)):
                dupes += 1
                candidate_basename = "{}_{}".format(converted_basename, dupes)
            converted_basename = candidate_basename
        converted_filename = "{}.csv".format(converted_basename)
        df[cols].to_csv(settings.INTERMEDIATE_DIR / converted_filename, index=False)
        return converted_filename
