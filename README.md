# Overview

This repository contains scripts, and per-lab code/configuration,
suitable for normalising and anonymising lab data, and combining it
into a single zipped CSV file, suitable for consumption by the
`openpath-dash` app.

Each lab's configuration is represented by a folder / python module
witin `data_sources/`, along with a bit of config in `lib.settings`

# Refreshing external data depencenices

    PYTHONPATH=.  python runner.py fetch

And then commit the resulting files (in `final_data/`).


# Data source anonymisation

To run the cornwall processor, for example:

    PYTHONPATH=. DATA_BASEDIR=/mnt/secure_private_data python runner.py process cornwall

To run against sample / test data (no-multiprocessing makes debugging easier):

    PYTHONPATH=. LOG_LEVEL=DEBUG python runner.py process cambridge --no-multiprocessing --reimport --single-file=data_sources/cambridge/example.csv

The runner is idempotent; current progress is (awkwardly) recorded in
a SQLite database and files in `intermediate_files/`. Only new,
unprocessed files are processed in a normal run. A `--reimport` switch
indicates everything should be wiped and started from the beginnging

Successul runs finish with most intermediate files being deleted;
however, files named `intermediate_files/combined_<lab_id>.csv` are
kept after each run, so new data can be appended to these files
incrementally.  These files are kept because whole-dataset operations
(e.g. low number suppression) must be run after each new set of data
is appended


### Intermediate file tracking

The awkwardness mentioned above is an artefact of it being useful to
split operations into monthly files during the development process,
and not having time to furter refactor. If we return to this project,
I'd start by refactoring the intermediate file tracking.

To summarise what can end up in there:

* `converted_*` are the outputs of each file having been processed. These are stored in INTERMEDIATE_DIR and recored in sqlite, and deleted when they've been `merged` (see the next step)
* These individual files are combined into a single `combined` file and marked in sqlite as `merged`.
* Single `combined` files are anonymised and so on to a format suitable for the website, and output as `processed_*` files (one per lab)
* Each processed file is combined to an `all_processed.csv.zip` file in `final_data/`.


These files
should be places in the dokku storage for the openpath-dash app (at
the time of writing,
`root@dokku.embdatalab.net:/var/lib/dokku/data/storage/openpath-dash/data_csvs`)

# Making a  new data source

A data source is a package within `data_sources/`,
(e.g. `data_sources/north_devon/`)

It contains code and configuration that, given input data in a variety
of row-oriented formats containing one month of data per file,
converts it such that all raw test result values are aggregated to
"within range", "under range", "over range", or a number of error
codes indicating that the range could not be computed (for example,
because we currently don't consider children for reference ranges; or
because a value was non-numeric).


## Checklist for a new data source

The department manage a file hosting service called
[Filr](https://filr.imsu.ox.ac.uk) (credentials in LastPass). We
create a new folder for each new lab, and then grant external access
to one or more people in that lab to that folder.

Our VM is set up to sync data from filr to `/home/filr/`.

Once data is available there, do the following steps to develop a new data source:

* On the secure server:
  * Check you can extract raw files
  * Review file format. Make notes in a README in `data_sources/<lab_id>`.  This is easiest in a python console using pandas and dataframe inspection
  * If reference ranges are required, obtain a file, and create a normalised version (see [here](https://github.com/ebmdatalab/openpath-pipeline/blob/0d378e18b6581ecb1e588cb50d129487de927623/lib/intermediate_file_processing.py#L64-L74) for notes). This is currently done by Helen, and should be done using a script ([example](https://github.com/ebmdatalab/openpath-pipeline/blob/0d378e18b6581ecb1e588cb50d129487de927623/data_sources/cornwall/generate_ranges.py))
  * Save any supplementary files (reference ranges, test metadata) in the same location, and also note in the README
  * Make an anonymised sample, preferably by writing some code (see Exeter for an example):
    * Replace any patient identifiers with random strings (or one random string)
    * Jitter or randmise any patient ages by several years
    * Jitter any sample / result dates by several years
    * Shuffle or randomise any GP/surgery identifier columns
  * Copy this to a local file
* Test and develop a processing configuration
* When this works, generate a list of test codes (on secure server) to aid in test code normalisation. These should include test names where available.
* You may also need to generate a list of practice ids to map - if practice names are also available, this will help
* Create test code normalisation. This is currently done in [a google sheet](https://drive.google.com/drive/u/1/folders/1IptCY7S_32fGnxWQQJaN2p51b1phaeK1) by Helen; there are notes in the "procedure" worksheet as to how this has been done previously
* See if the whole thing runs on the secure server

## Data source processing walkthrough

A data source must a file `anonymiser_config.py`.  This file must contain:

* `LAB_CODE`: a string with a unique token for this source
* `REFERENCE_RANGES`: path to a CSV of reference ranges (this may be empty; see below)
* `INPUT_FILES`: an iterable returning input filenames
* `row_iterator(filename)`: a function that yields rows of dictionaries from a source pointed to by `filename`
* `drop_unwanted_data(row)`: a function that raises `StopProcessing` if the row passed in should be skipped (for example, invalid or dummy data)
* `normalise_data(row)`: a function that normalises an input row to an output row with the fields `month`, `test_code`, `test_result`, `practice_id`, `age`, `sex`, `direction`.
* `__init__.py` to make this a python module

Currently we only process data for adults (18 years and older), so at
the point age is calculated, rows should be dropped for under-18s (by
raising `StopIteration`).

For labs that integrate reference range values into the result data
file, `REFERENCE_RANGES` should be an empty string, and you will also
want to implement:

* `convert_to_result`: a function which takes a `row` dict
  corresponding to the output row generated by `normallise_data`, and
  sets a `result_category` key which is one of the preset "error
  codes" (`WITHIN_RANGE`, `ERR_INVALID_REF_RANGE`, etc). See the base
  implementation at
  `lib.intermediate_file_processing.standard_convert_to_result` for an
  example

A data source should also include a README, and any CSVs and other
related material to help developers understand the data.


## Reference Range CSVs

For labs which provide reference ranges in a file separate from the
data, we require CSV files which are generated in a normalised form
from source files stored in `<data_source>/metadata/`.  They must
contain the fields `test`, `min_adult_age`, `max_adult_age`, `low_F`,
`low_M`, `high_F`, `high_M`.

A test may appear several times for different age ranges. We use `120`
as the ceiling for adult ranges.

By convention, these CSVs are generated by a python file called
`generate_ranges.py`, within each source folder.  See
`data_sources/north_devon/generate_ranges.py` for an example.

The presence of a suitably-formatted file, referenced by the
`REFERENCE_RANGES` variable, means "error codes" can be calculated
automatically.


# Accessing our secure server

* Obtain MSD IT VPN credentials (these are the same as your Windows login credentials)
* Get a new account set up on the virtual machine `ebmdatalab.phc.ox.ac.uk`, by sending a request to ithelp@medsci.ox.ac.uk
  * Be sure this is described / permitted by our currently approved DPIA; if not, amend
* Connect to [MSD IT VPN](https://www.medsci.ox.ac.uk/divisional-services/support-services-1/information-technology/document-and-file-storage/vpn)
* SSH to `<username>@ebmdatalab.phc.ox.ac.uk`
