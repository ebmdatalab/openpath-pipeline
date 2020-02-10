from datetime import date
from dateutils import relativedelta
from pandas.api.types import CategoricalDtype
import datetime
import glob
import io
import os
import pandas as pd
import requests

from .intermediate_file_tracking import get_unmerged_filenames, mark_as_merged
from .settings import *


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
        ],
        ordered=False,
    )


INTERMEDIATE_OUTPUT_DTYPES = {
    "month": _date_dtype(),
    "test_code": str,
    "practice_id": str,
    "result_category": _result_dtype(),
}


categorical = CategoricalDtype(ordered=False)

FINAL_OUTPUT_DTYPES = {
    "ccg_id": categorical,
    "practice_id": categorical,
    "count": int,
    "error": int,
    "lab_id": categorical,
    "practice_name": categorical,
    "result_category": _result_dtype(),
    "test_code": categorical,
    "total_list_size": int,
}


def combine_csvs_to_dataframe(csv_filenames, dtypes):
    """Combine CSVs (which must include columns defined in `dtypes`)

    """
    unmerged = pd.read_csv(
        # Reading an empty CSV in this way allows us to define column
        # types for an empty dataframe, which we can use for `concat`
        # operations
        io.StringIO(""),
        names=dtypes.keys(),
        dtype=dtypes,
    )
    for filename in csv_filenames:
        unmerged = pd.concat(
            [
                unmerged,
                pd.read_csv(INTERMEDIATE_DIR / filename, na_filter=False, dtype=dtypes),
            ],
            sort=False,
        )
    return unmerged


def combine_and_append_csvs(lab):
    """For a given lab, combine any unmerged monthly files and append them
    to an an existing `combined` file.  Also sanity checks data to
    provide some assurance data hasn't been appended twice.

    """
    all_results_path = "{}combined_{}.csv".format(ENV, lab)

    # First, build a single dataframe of all the constituent monthly
    # CSVs that have not previously been processed
    unmerged_filenames = [x[1] for x in get_unmerged_filenames(lab)]
    unmerged = combine_csvs_to_dataframe(unmerged_filenames, INTERMEDIATE_OUTPUT_DTYPES)

    # Now open the any existing "combined" file and append our new rows to that
    try:
        existing = pd.read_csv(
            INTERMEDIATE_DIR / all_results_path,
            dtype=INTERMEDIATE_OUTPUT_DTYPES,
            na_filter=False,
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
        merged.to_csv(INTERMEDIATE_DIR / all_results_path, index=False)
    # Clean up unmerged files
    for filename in unmerged_filenames:
        mark_as_merged(lab, filename)
        os.remove(INTERMEDIATE_DIR / filename)
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


def estimate_errors(df):
    """Add a column indicating the "error" range for suppressed values
    """
    df["count"] = df["count"].replace("1-5", 3)
    df.loc[df["count"] == 3, "error"] = 2
    df["error"] = df["error"].fillna(0)
    df["count"] = pd.to_numeric(df["count"])
    return df


def trim_trailing_months(df):
    """There is often a lead-in to the available data. Filter out months
    which have less than 5% the max monthly test count
    """
    t2 = df.groupby(["month"])["count"].sum().reset_index().sort_values(by="count")
    t2 = t2.loc[(t2[("count")] > t2[("count")].max() * 0.05)]
    return df.merge(t2["month"].reset_index(drop=True), on="month", how="inner")


def get_practices():
    """Make a CSV of "standard" GP practices and list size data.
    """
    practices_url = (
        "https://openprescribing.net/api/1.0/org_code/?org_type=practice&format=csv"
    )
    target_path = FINAL_DIR / "practice_codes.csv"
    # For some reason delegating the URL-grabbing to pandas results in a 403
    df = pd.read_csv(io.StringIO(requests.get(practices_url).text), na_filter=False)
    df = df[df["setting"] == 4]
    stats_url = "https://openprescribing.net/api/1.0/org_details/?org_type=practice&keys=total_list_size&format=csv"
    df_stats = pd.read_csv(io.StringIO(requests.get(stats_url).text), na_filter=False)
    # Left join because we want to keep practices without populations
    # for calculating proportions
    df = df.merge(
        df_stats, left_on=["code"], right_on=["row_id"], how="left"
    ).sort_values(by=["code", "date"])
    df = df[["ccg", "code", "name", "date", "total_list_size"]]
    df.columns = ["ccg_id", "practice_id", "practice_name", "month", "total_list_size"]
    df.to_csv(target_path, index=False)


def trim_practices_and_add_population(df):
    """Remove practices unlikely to be normal GP ones
    """
    # 1. Join on practices table
    # 2. Remove practices with fewer than 1000 total tests
    # 3. Remove practices that are missing population data
    practices = pd.read_csv(FINAL_DIR / "practice_codes.csv", na_filter=False)
    practices["month"] = pd.to_datetime(practices["month"])
    df["month"] = pd.to_datetime(df["month"])
    return df.merge(
        practices,
        how="inner",
        left_on=["month", "practice_id"],
        right_on=["month", "practice_id"],
    )


def normalise_and_suppress(lab, merged, offline):
    """Given a lab id and a file containing all processed data, (a)
    normalise test codes so they are consistent through time (e.g. the
    code for HB in one lab might be HB1 in April and change to HB2 in
    May); (b) do low-number suppression against the entire dataset

    """
    anonymised_results_path = INTERMEDIATE_DIR / "{}processed_{}.csv".format(ENV, lab)
    normalised = _normalise_test_codes(lab, merged, offline)
    # We have to convert these columns to categories *after* all the
    # constituent files have been loaded, as only then are all the
    # categorical values known
    normalised["test_code"] = normalised["test_code"].astype(
        CategoricalDtype(ordered=False)
    )
    if len(normalised):
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
        aggregated = aggregated[
            ["month", "test_code", "practice_id", "result_category", "count"]
        ]
        aggregated["lab_id"] = lab
        aggregated = estimate_errors(aggregated)
        aggregated = trim_trailing_months(aggregated)
        # get_practices()  <-- XXX reinstate
        aggregated = trim_practices_and_add_population(aggregated)
        aggregated.to_csv(anonymised_results_path, index=False)
        return anonymised_results_path
    else:
        return None


def make_final_csv():
    filenames = glob.glob(str(INTERMEDIATE_DIR / "{}processed_*".format(ENV)))
    combined = combine_csvs_to_dataframe(filenames, FINAL_OUTPUT_DTYPES)
    combined.to_csv(FINAL_DIR / "all_processed.csv.zip", index=False)
    for filename in filenames:
        os.remove(filename)
    return FINAL_DIR / "all_processed.csv.zip"


def report_oddness():
    df = pd.read_csv(
        FINAL_DIR / "all_processed.csv.zip",
        na_filter=False,
        dtype=INTERMEDIATE_OUTPUT_DTYPES,
    )
    report = (
        df.query("result_category > 1")
        .groupby(["test_code", "lab_id", "result_category"])
        .count()
        .reset_index()[["result_category", "lab_id", "test_code", "month"]]
    )
    denominators = (
        df.groupby(["test_code", "lab_id"])
        .count()
        .reset_index()[["lab_id", "test_code", "month"]]
    )
    report = report.merge(
        denominators,
        how="inner",
        left_on=["test_code", "lab_id"],
        right_on=["test_code", "lab_id"],
    )
    report["percentage"] = report["month_x"] / report["month_y"]
    report["result_category"] = report["result_category"].replace(ERROR_CODE_NAMES)
    odd = report[report["percentage"] > 0.1]
    if len(odd):
        print("The following error codes are more than 10% of all the results:")
        print()
        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            print(odd[["result_category", "test_code", "lab_id", "percentage"]])
