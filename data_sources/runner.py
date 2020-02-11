import argparse
import os
import sys

from lib.file_processing import process_files

import importlib


try:
    ModuleNotFoundError
except NameError:
    ModuleNotFoundError = ImportError


def get_lab_configs():
    configs = {}
    for folder in os.listdir("."):
        if os.path.isdir(folder) and not folder.startswith("."):
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
    labs = get_lab_configs()
    choices = list(labs.keys()) + ["all"]
    parser.add_argument("lab", help="lab", choices=choices)
    parser.add_argument("--single-file", help="Process single input file")
    parser.add_argument(
        "--no-multiprocessing", help="Use multiprocessing", action="store_true"
    )
    parser.add_argument(
        "--test", help="Use test environment and file-naming", action="store_true"
    )
    parser.add_argument(
        "--reimport",
        help="Delete existing files and import everything from scratch",
        action="store_true",
    )
    parser.add_argument(
        "--yes",
        help="Avoid prompts by answering 'yes' to any questions",
        action="store_true",
    )
    parser.add_argument(
        "--offline",
        help="Run everything offline (don't fetch latest data)",
        action="store_true",
    )
    args = parser.parse_args()
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
        result = process_files(
            config.LAB_CODE,
            os.path.join(os.path.dirname(config.__file__), config.REFERENCE_RANGES),
            files,
            config.row_iterator,
            config.drop_unwanted_data,
            config.normalise_data,
            convert_to_result,
            multiprocessing=multiprocessing,
            reimport=args.reimport,
            offline=args.offline,
            yes=args.yes,
        )
        print(result)


if __name__ == "__main__":
    main()
