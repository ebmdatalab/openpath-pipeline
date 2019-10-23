import argparse
import os
import sys

from lib.anonymise import process_files
from pathlib import Path
import importlib


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
            except ModuleNotFoundError:
                pass
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
        "--multiprocessing", help="Use multiprocessing", action="store_true"
    )
    args = parser.parse_args()
    log_level = 50 - (args.verbose * 10)
    config = labs[args.lab]
    process_files(
        config.LAB_CODE,
        Path(config.__file__).parent / config.REFERENCE_RANGES,
        log_level,
        args.files,
        config.row_iterator,
        config.drop_unwanted_data,
        config.normalise_data,
        args.multiprocessing,
    )


if __name__ == "__main__":
    main()
