"""Functions for fetching external data sources

"""
import pandas as pd
import requests
from . import settings

from io import StringIO


def get_codes():
    """Make a CSV of all the normalised test codes and lab test codes that
    have been marked in the Google Sheet for export.

    """
    url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSeLPEW4rTy_hCktuAXEsXtivcdREDuU7jKfXlvJ7CTEBycrxWyunBWdLgGe7Pm1A/pub?gid=241568377&single=true&output=csv"
    target_path = settings.FINAL_DIR / "test_codes.csv"
    df = pd.read_csv(url)
    df[df["show_in_app?"] == True].to_csv(target_path, index=False)


def get_practices():
    """Make a CSV of "standard" GP practices and list size data.
    """
    practices_url = (
        "https://openprescribing.net/api/1.0/org_code/?org_type=practice&format=csv"
    )
    target_path = settings.FINAL_DIR / "practice_codes.csv"
    # For some reason delegating the URL-grabbing to pandas results in a 403
    df = pd.read_csv(StringIO(requests.get(practices_url).text))
    df = df[df["setting"] == 4]
    stats_url = "https://openprescribing.net/api/1.0/org_details/?org_type=practice&keys=total_list_size&format=csv"
    df_stats = pd.read_csv(StringIO(requests.get(stats_url).text))
    # Left join because we want to keep practices without populations
    # for calculating proportions
    df = df.merge(
        df_stats, left_on=["code"], right_on=["row_id"], how="left"
    ).sort_values(by=["code", "date"])
    df = df[["ccg", "code", "name", "date", "total_list_size"]]
    df.columns = ["ccg_id", "practice_id", "practice_name", "month", "total_list_size"]
    df.to_csv(target_path, index=False)
