# Plymouth Lab Data

## Main data table

Supplied in the format of zip files, each of which contains data for that quarter.

## Lookup tables

There are no lookup tables as the lab has provided a range for each test within the results.

## Generating anonymised dataset

First, generate a reference ranges file.

    python generate_ranges.py metadata/TestFormatRanges.csv

Next, apply these ranges to the input data, suppress any identifying columns, and suppress low numbers:

    python generate_anonymised.py path_to_csvs/*zip

Be sure only to run this within a secure environment.
