import argparse
import os
import sys

from lib.file_processing import process_files
from lib.fetchers import get_codes
from lib.fetchers import get_practices
from lib.whole_file_processing import (
    combine_and_append_csvs,
    normalise_and_suppress,
    make_final_csv,
)
import importlib


try:
    ModuleNotFoundError
except NameError:
    ModuleNotFoundError = ImportError


def get_lab_configs():
    configs = {}
    for folder in os.listdir("data_sources/"):
        if os.path.isdir("data_sources/" + folder) and not folder.startswith("."):
            try:
                config = importlib.import_module(
                    "data_sources.{folder}.anonymiser_config".format(folder=folder)
                )
                if config:
                    error = False
                    for required in [
                        "LAB_CODE",
                        "REFERENCE_RANGES",
                        "row_iterator",
                        "drop_unwanted_data",
                        "normalise_data",
                    ]:
                        if not hasattr(config, required):
                            error = True
                            print(
                                "Error: data_sources.{folder}.anonymiser_config lacks required attribute {required}".format(
                                    folder=folder, required=required
                                )
                            )
                    if error:
                        sys.exit(1)
                    if config.LAB_CODE in configs:
                        print(
                            "Error: more than one definition for LAB_CODE {lab_code}".format(
                                lab_code=config.LAB_CODE
                            )
                        )
                    configs[config.LAB_CODE] = config
            except ModuleNotFoundError as e:
                if not e.name.endswith("anonymiser_config"):
                    raise
    return configs


def main():
    # I want a method to fetch practices and test codes, and also to run the data-spy thing
    labs = get_lab_configs()
    choices = list(labs.keys()) + ["all"]
    parser = argparse.ArgumentParser(
        description="Tools to generate suitably anonymised subset of raw input data"
    )

    subparsers = parser.add_subparsers()
    process = subparsers.add_parser("process", help="Process lab files")
    process.set_defaults(command=do_process)
    fetch = subparsers.add_parser(
        "fetch", help="Fetch latest versions of metadata files"
    )
    fetch.set_defaults(command=do_fetch)
    process.add_argument("lab", help="lab", choices=choices)
    process.add_argument("--single-file", help="Process single input file")
    process.add_argument(
        "--no-multiprocessing", help="Use multiprocessing", action="store_true"
    )
    process.add_argument(
        "--test", help="Use test environment and file-naming", action="store_true"
    )
    process.add_argument(
        "--reimport",
        help="Delete existing files and import everything from scratch",
        action="store_true",
    )
    process.add_argument(
        "--yes",
        help="Avoid prompts by answering 'yes' to any questions",
        action="store_true",
    )
    config = parser.parse_args()
    try:
        config.command(config)
    except AttributeError:
        parser.print_help()


def do_fetch(args):
    get_codes()
    get_practices()


def do_process(args):
    labs = get_lab_configs()
    multiprocessing = not args.no_multiprocessing
    if args.test:
        os.environ["OPATH_ENV"] = "test_"
    else:
        os.environ["OPATH_ENV"] = ""
    if args.lab == "all":
        labs_to_process = list(labs.keys())
    else:
        labs_to_process = [args.lab]
    for lab in labs_to_process:
        config = labs[lab]
        print("Processing {lab}".format(lab=lab))
        if args.single_file:
            files = [args.single_file]
        else:
            files = config.INPUT_FILES
            assert config.INPUT_FILES, "No input files found"
        if hasattr(config, "convert_to_result"):
            convert_to_result = config.convert_to_result
        else:
            convert_to_result = None
        process_files(
            config.LAB_CODE,
            os.path.join(os.path.dirname(config.__file__), config.REFERENCE_RANGES),
            files,
            config.row_iterator,
            config.drop_unwanted_data,
            config.normalise_data,
            convert_to_result,
            multiprocessing=multiprocessing,
            reimport=args.reimport,
            yes=args.yes,
        )
    done_something = False
    for lab in labs.keys():
        merged = combine_and_append_csvs(lab)
        done_something = normalise_and_suppress(lab, merged) or done_something
    combined = make_final_csv()
    if done_something:
        print("Final data at {}".format(combined))
    else:
        print("No data written")


if __name__ == "__main__":
    main()
