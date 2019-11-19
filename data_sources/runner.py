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
                config = importlib.import_module(f"{folder}.anonymiser_config")
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
                                f"Error: {folder}.anonymiser_config lacks required attribute {required}"
                            )
                    if error:
                        sys.exit(1)
                    if config.LAB_CODE in configs:
                        print(
                            f"Error: more than one definition for LAB_CODE {config.LAB_CODE}"
                        )
                    configs[config.LAB_CODE] = config
            except ModuleNotFoundError as e:
                print(e.name)
                if not e.name.endswith("anonymiser_config"):
                    raise
    return configs


def main():
    parser = argparse.ArgumentParser(
        description="Generate suitably anonymised subset of raw input data"
    )
    labs = list_labs()
    parser.add_argument("lab", help="lab", choices=labs.keys())
    parser.add_argument("files", nargs="+", help="Monthly input files")
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
    args = parser.parse_args()
    multiprocessing = not args.no_multiprocessing
    log_level = 50 - (args.verbose * 10)
    config = labs[args.lab]
    process_files(
        config.LAB_CODE,
        os.path.join(os.path.dirname(config.__file__), config.REFERENCE_RANGES),
        log_level,
        args.files,
        config.row_iterator,
        config.drop_unwanted_data,
        config.normalise_data,
        multiprocessing,
    )


if __name__ == "__main__":
    main()
