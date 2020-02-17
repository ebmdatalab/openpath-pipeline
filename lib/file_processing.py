from functools import partial
from multiprocessing import Pool
import glob
import os

from .whole_file_processing import (
    combine_and_append_csvs,
    normalise_and_suppress,
    make_final_csv,
)
from .intermediate_file_tracking import reset_lab, get_processed_filenames

from .intermediate_file_processing import make_intermediate_file

from . import settings


def process_files(
    lab,
    reference_ranges,
    filenames,
    row_iterator,
    drop_unwanted_data,
    normalise_data,
    convert_to_result,
    multiprocessing=False,
    reimport=False,
    yes=False,
):
    """Process (normalise and anonymise) a list of filenames, using custom
    functions that are passed in from per-lab configurations.

    Any filenames already processed are skipped.

    """
    if reimport:
        do_reset = False
        if yes:
            do_reset = True
        else:
            really_reset = input("Really reset all data? (y/n)")
            if really_reset == "y":
                do_reset = True
        if do_reset:
            reset_lab(lab)
            # Delete everything, including the merged intermediate
            # files that are a running record of what's been done so
            # far
            target_filenames = glob.glob(
                str(settings.INTERMEDIATE_DIR / "{}*_{}*.csv".format(settings.ENV, lab))
            )
            for target_filename in target_filenames:
                os.remove(target_filename)
        else:
            return
    filenames = sorted(filenames)
    seen_filenames = get_processed_filenames(lab)
    filenames = set(filenames) - set(seen_filenames)
    if filenames:
        make_intermediate_file_partial = partial(
            make_intermediate_file,
            lab,
            reference_ranges,
            row_iterator,
            drop_unwanted_data,
            normalise_data,
            convert_to_result=convert_to_result,
        )
        if multiprocessing:
            with Pool() as pool:
                pool.map(make_intermediate_file_partial, filenames)
        else:
            for f in filenames:
                make_intermediate_file_partial(f)
        merged = combine_and_append_csvs(lab)
        finished = normalise_and_suppress(lab, merged)
        combined = make_final_csv()
        if finished:
            return "Final data at {}".format(combined)
        else:
            return "No data written"
    else:
        return "Nothing to do"
