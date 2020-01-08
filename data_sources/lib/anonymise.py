"""Generates anonymised datasets from input files and JSON-based configurations.

Currently only works for XLS formatted inputs without column headers
"""
import datetime
import csv
import json
import glob
import io
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

DATE_FLOOR = (date.today() - relativedelta(years=5)).strftime("%Y/%m/%d")

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
    return os.environ.get("OPATH_ENV", "")


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
            get_env(), self.lab, date_collected.replace("/", "_")
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
        df[cols].to_csv(converted_filename, index=False)
        return converted_filename


def _date_dtype():
    # Build categorical values for months
    month = datetime.date(2014, 1, 1)
    month_categories = []
    while month <= date.today():
        month_categories.append(month.strftime("%Y/%m/%d"))
        month += relativedelta(months=1)
    return CategoricalDtype(categories=month_categories, ordered=False)


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
            ERR_NO_TEST_CODE,
        ],
        ordered=False,
    )


def combine_csvs(lab):
    """For a given lab, combine any unmerged monthly files and append them
    to an an existing `combined` file.  Also sanity checks data to
    provide some assurance data hasn't been appended twice.

    """
    all_results_path = "{}combined_{}.csv".format(get_env(), lab)

    # First, build a single dataframe of all the constituent monthly
    # CSVs that have not previously been processed
    processed_data_dtypes = {
        "month": _date_dtype(),
        "test_code": str,
        "practice_id": str,
        "result_category": _result_dtype(),
    }
    unmerged_filenames = get_unmerged_filenames(lab)
    unmerged = pd.read_csv(
        # Reading an empty CSV in this way allows us to define column
        # types for an empty dataframe, which we can use for `concat`
        # operations
        io.StringIO(""),
        names=REQUIRED_NORMALISED_KEYS,
        dtype=processed_data_dtypes,
    )
    for source_filename, converted_filename in unmerged_filenames:
        unmerged = pd.concat(
            [
                unmerged,
                pd.read_csv(
                    converted_filename, na_filter=False, dtype=processed_data_dtypes
                ),
            ]
        )

    # Now open the any existing "combined" file and append our new rows to that
    try:
        existing = pd.read_csv(
            all_results_path, dtype=processed_data_dtypes, na_filter=False
        )
        # Test we're not re-appending rows to the same file. In theory
        # this shouldn't happen as we track imported filenames, but
        # until that code is tested and known to be rebust: has the
        # number of tests in the previously-most-recent month stayed
        # within 20% of previous value?
        assert (
            len(existing[pd.isnull(existing["month"])]) == 0
        ), "There are `nan` values for month"
        month_vals = sorted(existing["month"].unique())
        final_month = month_vals[-1]
        final_count = existing[existing["month"] == final_month].count().iloc[0]
        if unmerged_filenames:
            merged = pd.concat([existing, unmerged], sort=False)
        else:
            merged = existing
        new_final_count = merged[merged["month"] == final_month].count().iloc[0]
        assert (new_final_count - final_count) < 0.2 * final_count, (
            "Number of tests in month {} increased by more than 20%".format(final_month)
            # Why 20%? Normally data is provided as one file per
            # month, but at some month boundaries (e.g. Dec/Jan) it's
            # not unusual to have a load of tests ordered in one month
            # and reported on in the next
        )
    except FileNotFoundError:
        # The first time we've made a merged file
        merged = unmerged
    if unmerged_filenames:
        merged.to_csv(all_results_path, index=False)
    # Clean up unmerged files
    for _, filename in unmerged_filenames:
        mark_as_merged(lab, filename)
        # xxx REINSTATE XXX os.remove(filename)
    # Thes columns can't be categorical up-front as we don't know what
    # practice ids or test codes are going to be present until thie end
    merged["practice_id"] = merged["practice_id"].astype(
        CategoricalDtype(ordered=False)
    )
    merged["test_code"] = merged["test_code"].astype(CategoricalDtype(ordered=False))
    return merged


CODE_MAPPINGS = {
    "nd": ["nd_testcode"],
    "cornwall": ["cornwall_testcode"],
    "plymouth": ["plym_testcode", "other_plym_codes"],
    "cambridge": [],
}


def _get_test_codes(lab, offline):
    """Make a CSV of all the normalised test codes and lab test codes that
    have been marked in the Google Sheet for export.

    """
    if offline:
        uri = "test_codes.csv"
    else:
        uri = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSeLPEW4rTy_hCktuAXEsXtivcdREDuU7jKfXlvJ7CTEBycrxWyunBWdLgGe7Pm1A/pub?gid=241568377&single=true&output=csv"
    columns = CODE_MAPPINGS[lab] + ["datalab_testcode"]
    df = pd.read_csv(
        uri, na_filter=False, usecols=columns + ["show_in_app?", "testname"]
    )
    if not offline:
        df.to_csv("test_codes.csv", index=False)
    df = df[df["show_in_app?"] == True]

    # Drop any mappings that are actually the same as the datalab one
    for colname in CODE_MAPPINGS[lab]:
        df.loc[df[colname] == df["datalab_testcode"], colname] = "_DONTJOIN_"

    dupe_codes = df.datalab_testcode[df.datalab_testcode.duplicated()]
    dupe_names = df.testname[df.testname.duplicated()]
    if not dupe_codes.empty or not dupe_names.empty:
        raise ValueError(
            f"Non-unique test codes or names\n"
            f" codes: {', '.join(dupe_codes)}\n"
            f" names: {', '.join(dupe_names)}"
        )
    return df[columns]


def _normalise_test_codes(lab, df, offline):
    """Convert local test codes into a normalised version.

    """
    orig_cols = df.columns
    # test_code_mapping contains columns referenced in CODE_MAPPINGS
    # such that `datalab_testcode` is the canonical code, and each
    # extra column is a possible alias. These aliases are imputed by
    # hand and recorded in a Google Sheet; @helenCEBM is in the
    # process of documenting this.

    test_code_mapping = _get_test_codes(lab, offline)
    output = pd.DataFrame(columns=orig_cols)
    # For each test code identified for the lab in our
    # manually-curated mapping spreadsheet, rename any codes to our
    # normalised `datalab_testcode`. In addition, be sure also to
    # match on any codes in the lab data which are exactly the same as
    # the `datalab_testcode`.
    for colname in CODE_MAPPINGS[lab] + ["datalab_testcode"]:
        result = df.merge(
            test_code_mapping, how="inner", left_on="test_code", right_on=colname
        )
        result = result.rename(
            columns={"test_code": "source_test_code", "datalab_testcode": "test_code"}
        )
        output = output.append(result[orig_cols])
    return output


def normalise_and_suppress(lab, merged, offline):
    """Given a lab id and a file containing all processed data, (a)
    normalise test codes so they are consistent through time (e.g. the
    code for HB in one lab might be HB1 in April and change to HB2 in
    May); (b) do low-number suppression against the entire dataset

    """
    anonymised_results_path = "{}anonymised_{}.csv".format(get_env(), lab)
    normalised = _normalise_test_codes(lab, merged, offline)
    # We have to convert these columns to categories *after* all the
    # constituent files have been loaded, as only then are all the
    # categorical values known
    normalised["test_code"] = normalised["test_code"].astype(
        CategoricalDtype(ordered=False)
    )
    # Aggregate data to produce counts, and suppress low numbers.
    normalised.loc[:, "count"] = 0
    aggregated = (
        normalised.groupby(
            ["month", "test_code", "practice_id", "result_category"], observed=True
        )
        .count()
        .dropna()
    ).reset_index()
    aggregated.loc[aggregated["count"] < SUPPRESS_UNDER, "count"] = SUPPRESS_STRING
    aggregated[
        ["month", "test_code", "practice_id", "result_category", "count"]
    ].to_csv(anonymised_results_path, index=False)


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
    offline=False,
):
    if reimport:
        really_reset = input("Really reset all data? (y/n)")
        if really_reset == "y":
            reset_lab(lab)
            target_filenames = glob.glob("{}*{}.csv".format(get_env(), lab))
            for target_filename in target_filenames:
                if os.path.exists(target_filename):
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
        merged = combine_csvs(lab)
        normalise_and_suppress(lab, merged, offline)
    else:
        print("Nothing to do")
