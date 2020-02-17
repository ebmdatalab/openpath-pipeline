"""Script to generate synthetic data for testing

Run as `python make_extract.py <path_to_xlsx>`

"""

import datetime
import random
import pandas as pd

SAMPLE_SURGERIES = [
    "L83058",  # standard practice
    "82",  # branch for which we should have mapping
    "Y01050",  # standard practice (?)
    "BELV",  # mental health, should not be included
]

SAMPLE_AGES = [
    "55y",
    "20y",
    "11y 2m",
    "40y",
    "120y",
    "44y",
    "70y",
    "30y",
]  # 1 child (should be excluded)


def random_date(start, end):
    """
    Return a random datetime between two datetime objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + datetime.timedelta(seconds=random_second)


def anonymise(row):
    start = datetime.date.today() - datetime.timedelta(days=600)
    end = datetime.date.today()
    request_date = random_date(start, end)
    row.Date_Request_Made = request_date.strftime("%Y-%m-%d 00:00:00")
    row.Time_Request_Made = request_date.strftime("%H%M")
    row.Date_Specimen_Collected = request_date.strftime("%Y-%m-%d 00:00:00")
    row.Date_Specimen_Received = request_date.strftime("%Y-%m-%d 00:00:00")
    row.Date_Test_Performed = request_date.strftime("%Y-%m-%d 00:00:00")
    row.Date_Approved = request_date.strftime("%Y-%m-%d 00:00:00")

    row.Patients_Number = "P01234567"
    row.Specimen_Number = "CH01234/0123S"
    row.Requesting_Organisation_Code = random.choice(SAMPLE_SURGERIES)
    row.Requesting_Organisation_Desc = "doesnt matter"
    row["Age_on_Date_Request_Rec'd"] = random.choice(SAMPLE_AGES)
    row.Sex = "F"
    return row


def main():
    import sys

    fname = sys.argv[1]
    df = pd.read_excel(fname, dtype=str, na_filter=False)
    df = df.sample(50)
    df = df.apply(anonymise, axis=1, result_type="expand")
    df.to_excel("sample_" + fname, index=False, na_rep="")


if __name__ == "__main__":
    main()
