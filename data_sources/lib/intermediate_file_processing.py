"""Generates anonymised datasets from input files and JSON-based configurations.

Currently only works for XLS formatted inputs without column headers
"""
from collections import Counter
from functools import lru_cache
import csv
import os
import tempfile

from . import settings
from .intermediate_file_tracking import mark_as_processed

from .logger import log_info, log_warning


class StopProcessing(Exception):
    pass


@lru_cache(maxsize=1)
def get_ref_ranges(path):
    """Load a CSV of reference ranges into a list of dicts
    """
    required_cols = [
        "test",
        "min_adult_age",
        "max_adult_age",
        "low_F",
        "low_M",
        "high_F",
        "high_M",
    ]
    with open(path, newline="", encoding="ISO-8859-1") as f:
        lines = sorted(list(csv.DictReader(f)), key=lambda x: x["test"])
    assert sorted(lines[0].keys()) == sorted(
        required_cols
    ), "CSV at {} must define columns {}".format(path, required_cols)
    return lines


# Cache the fact any reference ranges are missing
NO_REF_RANGES = set()


def skip_old_data(row):
    if row["month"] < settings.DATE_FLOOR.strftime("%Y/%m/%d"):
        raise StopProcessing()


def standard_convert_to_result(row, ranges):
    """Given a row and a list of reference ranges, set a value of the
    `result_category` key in the `row` dict, and return that row

    A row is a dict with these keys:

        [month, test_code, practice_id, age, sex, direction].

    Every data source is expected to (and by this point, already
    validated to) return these keys.

    A list of reference ranges is a list where each element is a dict
    with the keys

        ["test", "min_adult_age", "max_adult_age", "low_F", "low_M", "high_F", "high_M"]

    This function uses these two variables together to work out the
    result_category. It turns out several data sources don't come with
    reference ranges, but do supply a within/outside range
    indicator. In such cases, this function is replaced by something
    much simpler, which just converts their indicator to our category
    codes.

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


def make_intermediate_file(
    filename,
    lab,
    reference_ranges,
    row_iterator,
    drop_unwanted_data,
    normalise_data,
    convert_to_result=None,
):
    """Given a filename, lab id, and reference ranges, create an
    intermediate file which is a normalised version of the original
    input file.

    'Normalised' means a version with minimum columns required for
    processing, practice ids converted to ODS codes, bad and old data
    dropped, months converted to YYYY/MM/DD, and all columns named in
    a consistent manner.

    Intermediate files are combined and anonymised later in the
    pipeline.

    """
    outfile = tempfile.NamedTemporaryFile(mode="w", delete=False)
    convert_to_result = convert_to_result or standard_convert_to_result
    if os.path.isfile(reference_ranges):
        ref_ranges = get_ref_ranges(reference_ranges)
    else:
        ref_ranges = []

    writer = csv.writer(outfile)
    first_dates = Counter()
    validated = False

    # Execute a range of operations, per-row
    for i, row in enumerate(row_iterator(filename)):
        try:
            drop_unwanted_data(row)
            row = normalise_data(row)
            skip_old_data(row)
            row = convert_to_result(row, ref_ranges)
        except StopProcessing:
            row = None

        if row:
            if not validated:
                writer.writerow(settings.REQUIRED_NORMALISED_KEYS)
                # Check all the required keys have been provided
                # (in the first row only)
                provided_keys = set(row.keys())
                required_keys = set(settings.REQUIRED_NORMALISED_KEYS)
                missing_keys = required_keys - provided_keys
                assert not missing_keys, "Required keys missing: {}".format(
                    missing_keys
                )
                validated = True
            if i < 1000:
                # find most common date in this file, for naming
                first_dates[row["month"]] += 1
            # Only output the columns we care about
            subset = [row[k] for k in settings.REQUIRED_NORMALISED_KEYS]
            writer.writerow(subset)
    outfile.flush()

    # Compute an unused filename that reflects its contents to some degree
    most_common_date = first_dates.most_common(1)[0][0]
    converted_basename = "{}converted_{}_{}".format(
        settings.ENV, lab, most_common_date.replace("/", "_")
    )
    dupes = 0
    if os.path.exists(settings.INTERMEDIATE_DIR / "{}.csv".format(converted_basename)):
        dupes += 1
        candidate_basename = "{}_{}".format(converted_basename, dupes)
        while os.path.exists(
            settings.INTERMEDIATE_DIR / "{}.csv".format(candidate_basename)
        ):
            dupes += 1
            candidate_basename = "{}_{}".format(converted_basename, dupes)
        converted_basename = candidate_basename
    converted_filename = "{}.csv".format(converted_basename)
    converted_filepath = str(settings.INTERMEDIATE_DIR / converted_filename)
    os.rename(outfile.name, converted_filepath)
    mark_as_processed(lab, filename, converted_filepath)
    return converted_filepath
