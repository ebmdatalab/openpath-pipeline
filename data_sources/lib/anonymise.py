"""Generates anonymised datasets from input files and JSON-based configurations.

Currently only works for XLS formatted inputs without column headers
"""
import csv
import json
import glob
import os
from multiprocessing import Pool
import logging
import pandas as pd
from functools import partial
from functools import lru_cache

from .settings import *
from .whole_file_processing import (
    combine_and_append_csvs,
    normalise_and_suppress,
    make_final_csv,
)
from .intermediate_file_tracking import (
    mark_as_processed,
    reset_lab,
    get_processed_filenames,
)


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


class RowAnonymiser:
    def __init__(
        self,
        lab,
        ranges,
        drop_unwanted_data,
        normalise_data,
        convert_to_result,
        log_level=None,
    ):
        self.orig_row = None
        self.row = None
        self.ranges = ranges

        self.drop_unwanted_data = drop_unwanted_data
        self.normalise_data = normalise_data
        self.custom_convert_to_result = convert_to_result
        # self.reference_ranges_path = reference_ranges_path

        streamhandler = logging.StreamHandler()
        logging.basicConfig(
            level=log_level,
            format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
            handlers=[streamhandler],
        )

        self.logger = logging.getLogger()

    def log(self, level, msg, *args):
        msg = msg + " %s "
        args = args + (json.dumps(self.orig_row),)
        getattr(self.logger, level)(msg, *args)

    def log_warning(self, msg, *args):
        return self.log("warning", msg, *args)

    def log_info(self, msg, *args):
        return self.log("info", msg, *args)

    def convert_to_result(self):
        """Set a value of the `result_category` key, based on existing fields:

        month, test_code, practice_id, age, sex, direction
        """
        test_code = self.row["test_code"]
        result = self.row["test_result"]
        sex = self.row["sex"]
        age = self.row["age"]
        direction = self.row["direction"]
        if test_code in NO_REF_RANGES:
            self.row["result_category"] = ERR_NO_REF_RANGE
            return self.row
        last_matched_test = None
        found = False
        return_code = None
        for ref_range in self.ranges:
            if ref_range["test"] == test_code:
                found = True
                if not isinstance(result, float):
                    self.log_info("Unparseable result")
                    return_code = ERR_UNPARSEABLE_RESULT
                    break
                high = low = None
                if last_matched_test and last_matched_test != ref_range["test"]:
                    # short-circuit as the rows should be sorted by test
                    self.log_info("No matching ref range found")
                    return_code = ERR_NO_REF_RANGE
                    break
                last_matched_test = ref_range["test"]
                if age >= int(float(ref_range["min_adult_age"])) and age < int(
                    float(ref_range["max_adult_age"])
                ):
                    # matched the age
                    if sex == "M":
                        if ref_range["low_M"] and ref_range["high_M"]:
                            low = float(ref_range["low_M"])
                            high = float(ref_range["high_M"])
                    elif sex == "F":
                        if ref_range["low_F"] and ref_range["high_F"]:
                            low = float(ref_range["low_F"])
                            high = float(ref_range["high_F"])
                    else:
                        return_code = ERR_INVALID_SEX
                        self.log_info("Invalid sex %s", sex)
                        break
                    if (
                        low != ""
                        and high != ""
                        and low is not None
                        and high is not None
                    ):
                        if result > high:
                            if direction == "<":
                                self.log_warning(
                                    "Over range %s but result <; invalid", high
                                )
                                return_code = ERR_INVALID_RANGE_WITH_DIRECTION
                                break
                            else:
                                self.log_info("Over range %s", high)
                                return_code = OVER_RANGE
                                break
                        elif result < low:
                            if direction == ">":
                                self.log_warning("Under range %s but >; invalid", high)
                                return_code = ERR_INVALID_RANGE_WITH_DIRECTION
                                break
                            else:
                                self.log_info("Under range %s", low)
                                return_code = UNDER_RANGE
                                break
                        else:
                            if not direction or (
                                (direction == "<" and low == 0)
                                or (direction == ">" and high == RANGE_CEILING)
                            ):
                                self.log_info("Within range %s - %s", low, high)
                                return_code = WITHIN_RANGE
                                break
                            else:
                                self.log_warning(
                                    "Within range %s-%s but direction %s; invalid",
                                    low,
                                    high,
                                    direction,
                                )
                                return_code = ERR_INVALID_RANGE_WITH_DIRECTION
                                break

                    else:
                        return_code = ERR_INVALID_REF_RANGE
                        self.log_warning(
                            "Couldn't process ref range %s - %s", low, high
                        )
                        break
                else:
                    return_code = ERR_DISCARDED_AGE
        if not found:
            NO_REF_RANGES.add(test_code)
            self.log_info("Couldn't find ref range")
            return_code = ERR_NO_REF_RANGE
        self.row["result_category"] = return_code

    def skip_old_data(self):
        if self.row["month"] < DATE_FLOOR:
            raise StopProcessing()

    def process_row(self):
        try:
            self.drop_unwanted_data(self)
            self.normalise_data(self)
            self.skip_old_data()
            if self.custom_convert_to_result:
                self.custom_convert_to_result(self)
            else:
                self.convert_to_result()
        except StopProcessing:
            self.row = None

    def feed(self, row):
        self.orig_row = row
        self.row = row
        self.process_row()


class Anonymiser:
    def __init__(
        self,
        lab,
        reference_ranges,
        row_iterator=None,
        drop_unwanted_data=None,
        normalise_data=None,
        convert_to_result=None,
        log_level=logging.INFO,
    ):
        self.rows = []
        self.lab = lab
        self.row_iterator = row_iterator
        self.drop_unwanted_data = drop_unwanted_data
        self.normalise_data = normalise_data
        self.convert_to_result = convert_to_result
        self.normalise_data_checked = False
        self.log_level = log_level
        if os.path.isfile(reference_ranges):
            self.ref_ranges = get_ref_ranges(reference_ranges)
        else:
            self.ref_ranges = []

    def feed_file(self, filename):
        row_anonymiser = RowAnonymiser(
            self.lab,
            self.ref_ranges,
            self.drop_unwanted_data,
            self.normalise_data,
            self.convert_to_result,
            self.log_level,
        )
        for raw_row in self.row_iterator(filename):
            row_anonymiser.feed(raw_row)
            if row_anonymiser.row:
                if not self.normalise_data_checked:
                    provided_keys = set(row_anonymiser.row.keys())
                    required_keys = set(REQUIRED_NORMALISED_KEYS)
                    missing_keys = required_keys - provided_keys
                    assert not missing_keys, "Required keys missing: {}".format(
                        missing_keys
                    )
                    self.normalise_data_checked = True
                # Only output the columns we care about
                subset = [row_anonymiser.row[k] for k in REQUIRED_NORMALISED_KEYS]
                self.rows.append(subset)

    def to_csv(self):
        df = pd.DataFrame(columns=REQUIRED_NORMALISED_KEYS, data=self.rows)
        cols = ["month", "test_code", "practice_id", "result_category"]
        df["count"] = 1

        # Make a filename which reasonably represents the contents of
        # the file and doesn't already exist
        date_collected = (
            df.groupby("month").count()["test_code"].sort_values().index[-1]
        )
        converted_basename = "{}converted_{}_{}".format(
            ENV, self.lab, date_collected.replace("/", "_")
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
        df[cols].to_csv(INTERMEDIATE_DIR / converted_filename, index=False)
        return converted_filename


def process_file(
    lab,
    reference_ranges,
    log_level,
    row_iterator,
    drop_unwanted_data,
    normalise_data,
    convert_to_result,
    filename,
):
    anonymiser = Anonymiser(
        lab,
        reference_ranges=reference_ranges,
        row_iterator=row_iterator,
        drop_unwanted_data=drop_unwanted_data,
        normalise_data=normalise_data,
        convert_to_result=convert_to_result,
        log_level=log_level,
    )
    anonymiser.feed_file(filename)
    converted_filename = anonymiser.to_csv()
    mark_as_processed(lab, filename, converted_filename)


def process_files(
    lab,
    reference_ranges,
    log_level,
    filenames,
    row_iterator,
    drop_unwanted_data,
    normalise_data,
    convert_to_result,
    multiprocessing=False,
    reimport=False,
    offline=False,
):
    if reimport:
        really_reset = input("Really reset all data? (y/n)")
        if really_reset == "y":
            reset_lab(lab)
            target_filenames = glob.glob(
                str(INTERMEDIATE_DIR / "{}*_{}*.csv".format(ENV, lab))
            )
            for target_filename in target_filenames:
                os.remove(target_filename)
        else:
            return
    filenames = sorted(filenames)
    seen_filenames = get_processed_filenames(lab)
    filenames = set(filenames) - set(seen_filenames)
    if filenames:
        process_file_partial = partial(
            process_file,
            lab,
            reference_ranges,
            log_level,
            row_iterator,
            drop_unwanted_data,
            normalise_data,
            convert_to_result,
        )
        if multiprocessing:
            with Pool() as pool:
                pool.map(process_file_partial, filenames)
        else:
            for f in filenames:
                process_file_partial(f)
        merged = combine_and_append_csvs(lab)
        finished = normalise_and_suppress(lab, merged, offline)
        combined = make_final_csv()
        if finished:
            print("Final data at {}".format(combined))
        else:
            print("No data written")
    else:
        print("Nothing to do")
