import pandas as pd
import requests

import settings
import click

from io import StringIO


def get_practices():
    """Make a CSV of "standard" GP practices and list size data.
    """
    practices_url = (
        "https://openprescribing.net/api/1.0/org_code/?org_type=practice&format=csv"
    )
    target_path = settings.CSV_DIR / "practice_codes.csv"
    # For some reason delegating the URL-grabbing to pandas results in a 403
    df = pd.read_csv(StringIO(requests.get(practices_url).text), na_filter=False)
    df = df[df["setting"] == 4]
    stats_url = "https://openprescribing.net/api/1.0/org_details/?org_type=practice&keys=total_list_size&format=csv"
    df_stats = pd.read_csv(StringIO(requests.get(stats_url).text), na_filter=False)
    # Left join because we want to keep practices without populations
    # for calculating proportions
    df = df.merge(
        df_stats, left_on=["code"], right_on=["row_id"], how="left"
    ).sort_values(by=["code", "date"])
    df = df[["ccg", "code", "name", "date", "total_list_size"]]
    df.columns = ["ccg_id", "practice_id", "practice_name", "month", "total_list_size"]
    df.to_csv(target_path, index=False)


#####


def trim_practices_and_add_population(df):
    """Remove practices unlikely to be normal GP ones
    """
    # 1. Join on practices table
    # 2. Remove practices with fewer than 1000 total tests
    # 3. Remove practices that are missing population data
    practices = pd.read_csv(settings.CSV_DIR / "practice_codes.csv", na_filter=False)
    practices["month"] = pd.to_datetime(practices["month"])
    df["month"] = pd.to_datetime(df["month"])
    return df.merge(
        practices,
        how="inner",
        left_on=["month", "practice_id"],
        right_on=["month", "practice_id"],
    )


def normalise_practice_codes(df, lab_code):
    # XXX move to ND data processor
    if lab_code == "nd":
        prac = pd.read_csv(
            settings.CSV_DIR / "north_devon_practice_mapping.csv", na_filter=False
        )

        df3 = df.copy()
        df3 = df3.merge(
            prac, left_on="practice_id", right_on="LIMS code", how="inner"
        ).drop("LIMS code", axis=1)
        df3 = df3.loc[pd.notnull(df3["ODS code"])]
        df3 = df3.rename(
            columns={"practice_id": "old_practice_id", "ODS code": "practice_id"}
        ).drop("old_practice_id", axis=1)
        return df3
    else:
        return df


# Some of these should be done at data generation time, others prior
# to ingestion. For example, the anon_id stuff can happen separately
# as we don't want to run it as part of the expensive generation bits
# and it's not so sensitive


# Can be done earlier in pipeline
def estimate_errors(df):
    """Add a column indicating the "error" range for suppressed values
    """
    df["count"] = df["count"].replace("1-5", 3)
    df["count"] = df["count"].replace("1-6", 3)
    df.loc[df["count"] == 3, "error"] = 2
    df["error"] = df["error"].fillna(0)
    df["count"] = pd.to_numeric(df["count"])
    return df


def anonymise(df):
    df["practice_id"] = df.groupby("practice_id").ngroup()
    df["practice_name"] = df["practice_id"].astype(str) + " SURGERY"
    return df


def report_oddness(df):
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
    report["result_category"] = report["result_category"].replace(settings.ERROR_CODES)
    odd = report[report["percentage"] > 0.1]
    if len(odd):
        print("The following error codes are more than 10% of all the results:")
        print()
        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            print(odd[["result_category", "test_code", "lab_id", "percentage"]])


def normalise_and_trim(lab_code, filename):
    df = pd.read_csv(filename, na_filter=False)
    df = add_lab_code(df, lab_code)
    df = normalise_practice_codes(df, lab_code)
    df = estimate_errors(df)  # XXX can do this earlier in the pipeline
    df = trim_trailing_months(df)
    df = trim_practices_and_add_population(df)
    df = df[
        [
            "ccg_id",
            "count",
            "error",
            "lab_id",
            "month",
            "practice_id",
            "practice_name",
            "result_category",
            "test_code",
            "total_list_size",
        ]
    ]
    df.to_csv(settings.CSV_DIR / f"{lab_code}_processed.csv", index=False)


def combine(filenames):
    df = pd.DataFrame()
    for filename in filenames:
        if not filename.endswith("/all_processed.csv"):
            df = pd.concat([df, pd.read_csv(filename, na_filter=False)], sort=False)
    # df = anonymise(df)
    df.to_csv(
        settings.CSV_DIR / f"all_processed.csv.zip", index=False, compression="infer"
    )
