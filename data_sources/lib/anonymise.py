"""Generates anonymised datasets from input files and JSON-based configurations.

Currently only works for XLS formatted inputs without column headers
"""
import datetime
import csv
import json
import os
from dateutils import relativedelta
from datetime import date
from multiprocessing import Pool
import logging
import pandas as pd
from pandas.api.types import CategoricalDtype
from functools import partial
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


REQUIRED_NORMALISED_KEYS = ["month", "test_code", "practice_id", "result_category"]


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


def get_env():
    return os.environ.get("OPATH_ENV", "") + "_"


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

    def process_row(self):
        try:
            self.drop_unwanted_data(self)
            self.normalise_data(self)
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
            get_env(), self.lab, date_collected.replace("/", "_")
        )
        dupes = 0
        if os.path.exists("{}.csv".format(converted_basename)):
            dupes += 1
            candidate_basename = "{}_{}".format(converted_basename, dupes)
            while os.path.exists("{}.csv".format(candidate_basename)):
                dupes += 1
                candidate_filename = "{}_{}".format(converted_basename, dupes)
            converted_basename = candidate_filename
        converted_filename = "{}.csv".format(converted_basename)
        df[cols].to_csv(converted_filename, index=False)
        return converted_filename


def append_csvs(lab):
    outfile_path = "{}combined_{}.csv".format(get_env(), lab)
    unmerged = pd.DataFrame(columns=REQUIRED_NORMALISED_KEYS)
    unmerged_filenames = get_unmerged_filenames(lab)
    if not unmerged_filenames:
        print("Nothing to do")
        return

    # First, build a single dataframe of all the constituent monthly
    # CSVs. We use categorical types where possible to save memory on
    # the expensive grouping operation that comes next

    # Build categorical values for months
    month = datetime.date(2014, 1, 1)
    month_categories = []
    while month <= date.today():
        month_categories.append(month.strftime("%Y/%m/%d"))
        month += relativedelta(months=1)
    date_dtype = CategoricalDtype(categories=month_categories, ordered=False)

    result_dtype = CategoricalDtype(
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
            ERR_NO_TEST_CODE,
        ],
        ordered=False,
    )
    for source_filename, converted_filename in unmerged_filenames:
        unmerged = pd.concat(
            [
                unmerged,
                pd.read_csv(
                    converted_filename,
                    na_filter=False,
                    dtype={
                        "month": date_dtype,
                        "test_code": str,
                        "practice_id": str,
                        "result_category": result_dtype,
                    },
                ),
            ]
        )
    try:
        existing = pd.read_csv(outfile_path, na_filter=False)
        merged = pd.concat([existing, unmerged], sort=False)
    except FileNotFoundError:
        merged = unmerged
    # We have to convert these columns to categories *after* all the
    # constituent files have been loaded, as only then are all the
    # categorical values known
    merged["test_code"] = merged["test_code"].astype(CategoricalDtype(ordered=False))
    merged["practice_id"] = merged["practice_id"].astype(
        CategoricalDtype(ordered=False)
    )

    # Aggregate data to produce counts, and suppress low numbers
    aggregated = (
        merged.groupby(
            ["month", "test_code", "practice_id", "result_category"], observed=True
        )
        .count()
        .dropna()
    ).reset_index()
    aggregated["count"] = 0
    aggregated.loc[aggregated["count"] < SUPPRESS_UNDER, "count"] = SUPPRESS_STRING
    aggregated[
        ["month", "test_code", "practice_id", "result_category", "count"]
    ].to_csv(outfile_path, index=False)

    # Clean up unmerged files
    for _, filename in unmerged_filenames:
        mark_as_merged(lab, filename)
        os.remove(filename)


def get_engine():
    return create_engine("sqlite:///{}processed.db".format(get_env()))


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
        converted_at=datetime.datetime.now(),
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
        .values(merged_at=datetime.datetime.now())
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
        select([table.c.filename, table.c.converted_filename])
        .where(table.c.lab == lab)
        .where(table.c.merged_at == None)
    )
    result = conn.execute(s).fetchall()
    return [(x[0], x[1]) for x in result]


def reset_lab(lab):
    engine = get_engine()
    conn = engine.connect()
    table = get_processed_table(engine)
    conn.execute(table.delete().where(table.c.lab == lab))


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
):
    if reimport:
        really_reset = input("Really reset all data? (y/n)")
        if really_reset == "y":
            reset_lab(lab)
            target_filename = "combined_{}.csv".format(lab)
            if os.path.exists(target_filename):
                os.remove(target_filename)
        else:
            return
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
        convert_to_result,
    )
    if multiprocessing:
        with Pool() as pool:
            pool.map(process_file_partial, filenames)
    else:
        for f in filenames:
            process_file_partial(f)
    append_csvs(lab)
