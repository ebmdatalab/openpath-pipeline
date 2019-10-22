# Cornwall Lab Data

## Main data table

`sample.csv.zip` contains dummy data in the format supplied to us.  It
is in the format of monthly, zipped CSV files.

## Lookup tables

Supporting files supplied by the lab are in `metadata/`.

Some of these tables have duplicates. If using in pandas, be sure to
run `drop_duplicates()` on the dataframe first.

`TestLibraryCodes.csv` provides a mapping from test codes (as requested by
clinician and defined as `TestOrderCode` and `TestPanelCode` in the main
dataset) to a library entry (keyed by `TestLibraryCode`).

A library entry has a description (e.g. "Full Blood Count", a sample
type (`B` looks like blood - but we need another lookup table to be
sure), a library type (`P` looks like panel, `I`, `G`, `A` I don't
know)

`TestFormatCodes.csv` is a mapping from the result codes (as provided
by the lab and defined as `TestResultCode`) to a format entry which
provides a descriptive name, and the units.

`TestFormatRanges.csv` is the ranges file supplied to us.  Note it
does include historic ranges which may be relevant for applying at a
future date.

`SourceCodes` gives us names, addresses, and status of the places that
can request tests. We don't normally need this as the source data is
already mapped to practices for us.

## Generating anonymised dataset

First, generate a reference ranges file.

    python generate_ranges.py metadata/TestFormatRanges.csv

Next, apply these ranges to the input data, suppress any identifying columns, and suppress low numbers:

    python generate_anonymised.py path_to_csvs/*zip

Be sure only to run this within a secure environment.
