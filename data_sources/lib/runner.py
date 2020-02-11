from functools import partial
from multiprocessing import Pool
import glob

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

from .intermediate_file_processing import make_intermediate_file

from .settings import *


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
    converted_filename = make_intermediate_file(
        filename,
        lab,
        reference_ranges,
        row_iterator,
        drop_unwanted_data,
        normalise_data,
        convert_to_result,
    )
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
