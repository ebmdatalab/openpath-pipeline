"""Generates anonymised datasets from input files and JSON-based configurations.

Currently only works for XLS formatted inputs without column headers
"""
from datetime import datetime
import csv
import json
import os
from multiprocessing import Pool
import logging
import pandas as pd
from functools import partial
import glob
from functools import lru_cache
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, DateTime, MetaData, Index
from sqlalchemy.sql import and_
from sqlalchemy.sql import select

RANGE_CEILING = 99999

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
ERR_NO_TEST_CODE = 8


REQUIRED_NORMALISED_KEYS = [
    "month",
    "test_code",
    "test_result",
    "practice_id",
    "age",
    "sex",
    "direction",
    "result_category",
]


class StopProcessing(Exception):
    pass


@lru_cache(maxsize=1)
def get_ref_ranges(path):
    # columns must be ["test", "min_adult_age", "max_adult_age", "low_F", "low_M", "high_F", "high_M"]
    with open(path, newline="", encoding="ISO-8859-1") as f:
        lines = sorted(list(csv.DictReader(f)), key=lambda x: x["test"])
    return lines


# Cache the fact any reference ranges are missing
NO_REF_RANGES = set()


def configLogger():
    pass


class RowAnonymiser:
    def __init__(self, lab, ranges, drop_unwanted_data, normalise_data, log_level=None):
        self.orig_row = None
        self.row = None
        self.ranges = ranges

        self.drop_unwanted_data = drop_unwanted_data
        self.normalise_data = normalise_data
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

    def process_row(self):
        try:
            self.drop_unwanted_data(self)
            self.normalise_data(self)
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
        log_level=logging.INFO,
    ):
        self.rows = []
        self.lab = lab
        self.row_iterator = row_iterator
        self.drop_unwanted_data = drop_unwanted_data
        self.normalise_data = normalise_data
        self.normalise_data_checked = False
        self.log_level = log_level
        self.ref_ranges = get_ref_ranges(reference_ranges)

    def feed_file(self, filename):
        row_anonymiser = RowAnonymiser(
            self.lab,
            self.ref_ranges,
            self.drop_unwanted_data,
            self.normalise_data,
            self.log_level,
        )
        for raw_row in self.row_iterator(filename):
            row_anonymiser.feed(raw_row)
            if row_anonymiser.row:
                if not self.normalise_data_checked:
                    assert set(row_anonymiser.row.keys()) == set(
                        REQUIRED_NORMALISED_KEYS
                    )
                    self.normalise_data_checked = True

                self.rows.append(row_anonymiser.row)

    def to_csv(self):
        df = pd.DataFrame(self.rows)
        cols = ["month", "test_code", "practice_id", "result_category", "count"]
        df["count"] = 1

        # Suppress low numbers
        aggregated = (
            df.groupby(["month", "test_code", "practice_id", "result_category"])
            .count()
            .dropna()
        ).reset_index()
        aggregated.loc[aggregated["count"] < SUPPRESS_UNDER, "count"] = SUPPRESS_STRING

        # Make a filename which reasonably represents the contents of
        # the file and doesn't already exist
        date_collected = (
            aggregated.groupby("month").count()["test_code"].sort_values().index[-1]
        )
        converted_filename = "converted_{}_{}".format(self.lab, date_collected)
        dupes = 0
        while os.path.exists(f"{converted_filename}.csv"):
            dupes += 1
        if dupes:
            converted_filename = f"{converted_filename}_{dupes}"
        converted_filename = f"{converted_filename}.csv"
        aggregated[cols].to_csv(converted_filename, index=False)
        return converted_filename


def append_csvs(lab):
    outfile_path = "combined_{}.csv".format(lab)
    if not os.path.exists(outfile_path):
        add_header = True
    else:
        add_header = False
    count = 0
    with open(outfile_path, "a") as outfile:
        added_header = False
        for converted_filename in get_unmerged_filenames(lab):
            with open(converted_filename) as infile:
                for i, line in enumerate(infile):
                    if i == 0:
                        if add_header and not added_header:
                            outfile.write(line)
                            added_header = True
                        else:
                            continue
                    else:
                        outfile.write(line)
            mark_as_merged(lab, converted_filename)
            os.remove(converted_filename)
            count += 1
    if count:
        print("Combined {} data files at {}".format(count, outfile_path))
    else:
        print("No files to combine, nothing done")


def get_engine():
    return create_engine("sqlite:///processed.db")


def get_processed_table(engine):
    metadata = MetaData()
    processed = Table(
        "processed",
        metadata,
        Column("lab", String),
        Column("filename", String),
        Column("converted_filename", String),
        Column("converted_at", DateTime),
        Column("merged_at", DateTime),
        Index("idx_lab_filename", "lab", "filename", unique=True),
    )
    metadata.create_all(engine)
    return processed


def mark_as_processed(lab, filename, converted_filename):
    engine = get_engine()
    conn = engine.connect()
    ins = get_processed_table(engine).insert()
    conn.execute(
        ins,
        lab=lab,
        filename=filename,
        converted_filename=converted_filename,
        converted_at=datetime.now(),
    )


def mark_as_merged(lab, converted_filename):
    engine = get_engine()
    conn = engine.connect()
    table = get_processed_table(engine)
    conn.execute(
        table.update()
        .where(
            and_(table.c.lab == lab, table.c.converted_filename == converted_filename)
        )
        .values(merged_at=datetime.now())
    )


def get_processed_filenames(lab):
    engine = get_engine()
    conn = engine.connect()
    table = get_processed_table(engine)
    s = select([table.c.filename]).where(table.c.lab == lab)
    result = conn.execute(s).fetchall()
    return [x[0] for x in result]


def get_unmerged_filenames(lab):
    engine = get_engine()
    conn = engine.connect()
    table = get_processed_table(engine)
    s = (
        select([table.c.converted_filename])
        .where(table.c.lab == lab)
        .where(table.c.merged_at == None)
    )
    result = conn.execute(s).fetchall()
    return [x[0] for x in result]


def process_file(
    lab,
    reference_ranges,
    log_level,
    row_iterator,
    drop_unwanted_data,
    normalise_data,
    filename,
):
    anonymiser = Anonymiser(
        lab,
        reference_ranges=reference_ranges,
        row_iterator=row_iterator,
        drop_unwanted_data=drop_unwanted_data,
        normalise_data=normalise_data,
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
    multiprocessing=False,
):
    filenames = sorted(filenames)
    seen_filenames = get_processed_filenames(lab)
    filenames = set(filenames) - set(seen_filenames)
    process_file_partial = partial(
        process_file,
        lab,
        reference_ranges,
        log_level,
        row_iterator,
        drop_unwanted_data,
        normalise_data,
    )
    if multiprocessing:
        with Pool() as pool:
            pool.map(process_file_partial, filenames)
    else:
        for f in filenames:
            process_file_partial(f)
    append_csvs(lab)
