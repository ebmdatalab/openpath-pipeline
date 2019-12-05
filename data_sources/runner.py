import argparse
import os
import sys

from lib.anonymise import process_files
import importlib


try:
    ModuleNotFoundError
except NameError:
    ModuleNotFoundError = ImportError


def list_labs():
    configs = {}
    for folder in os.listdir("."):
        if os.path.isdir(folder):
            try:
                config = importlib.import_module(
                    "{folder}.anonymiser_config".format(folder=folder)
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
                                "Error: {folder}.anonymiser_config lacks required attribute {required}".format(
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
    parser = argparse.ArgumentParser(
        description="Generate suitably anonymised subset of raw input data"
    )
    labs = list_labs()
    choices = list(labs.keys()) + ["all"]
    parser.add_argument("lab", help="lab", choices=choices)
    parser.add_argument("--single-file", help="Process single input file")
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Logging verbosity; -v is ERROR, -vv WARNING, etc",
    )
    parser.add_argument(
        "--no-multiprocessing", help="Use multiprocessing", action="store_true"
    )
    parser.add_argument(
        "--reimport",
        help="Delete existing files and import everything from scratch",
        action="store_true",
    )
    args = parser.parse_args()
    multiprocessing = not args.no_multiprocessing
    log_level = 50 - (args.verbose * 10)
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
        if hasattr(config, "convert_to_result"):
            convert_to_result = config.convert_to_result
        else:
            convert_to_result = None
        process_files(
            config.LAB_CODE,
            os.path.join(os.path.dirname(config.__file__), config.REFERENCE_RANGES),
            log_level,
            files,
            config.row_iterator,
            config.drop_unwanted_data,
            config.normalise_data,
            convert_to_result,
            multiprocessing,
            args.reimport,
        )


if __name__ == "__main__":
    main()
