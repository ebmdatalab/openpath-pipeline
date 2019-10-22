import pandas as pd


# Biochemistry ranges
ref_range = pd.read_csv("metadata/B-RECORD-ARCHIVE-18.csv")

# Manaully converted to spreadsheet from word document, REF.RANGES.v11.2.doc by Helen
ref_range_haem = pd.read_csv("metadata/ND_haem_ref_ranges_v11.2.csv")

# Combined
ref = pd.concat([ref_range, ref_range_haem], sort=False).reset_index(drop=True)


## Rearrange and tidy ref ranges
ref["Sex"] = ref["Sex"].str.strip()

# create flag to indicate age ranges applicable to adults
ref["adult_range_flag"] = 0
ref.loc[
    (
        (ref["Age"].str[-7:] == "+ years")
        & (pd.to_numeric(ref["Age"].str[:2], errors="coerce") >= 15)
        & (pd.to_numeric(ref["Age"].str[:2], errors="coerce") <= 20)
    )
    | (ref["Age"] == "All"),
    "adult_range_flag",
] = 1
ref.loc[(ref["Age"] == "8+ years"), "adult_range_flag"] = 1


# find the minimum age we consider adult for each test
ref.loc[
    (ref["adult_range_flag"] == 1) & (ref["Age"] != "All"), "min_adult_age"
] = pd.to_numeric(ref["Age"].str[:2], errors="coerce")
ref.loc[(ref["adult_range_flag"] == 1) & (ref["Age"] == "All"), "min_adult_age"] = 0
ref.loc[(ref["Age"] == "8+ years"), "min_adult_age"] = pd.to_numeric(
    ref["Age"].str[:1], errors="coerce"
)

# Ensure ref ranges are numeric and not null
ref["low"] = pd.to_numeric(ref["Low"], errors="coerce")
ref["high"] = pd.to_numeric(ref["High"], errors="coerce")

# Only adults, because we've been advised children stuff is too difficult
# And skip reference ranges without low or high, because ?
ref = ref.loc[
    (ref["adult_range_flag"] == 1) & pd.notnull(ref["low"]) & pd.notnull(ref["high"])
]
ref = (
    ref[["Code", "Name", "min_adult_age", "Sex", "low", "high"]]
    .reset_index()
    .drop("index", axis=1)
)
ref = ref.set_index(["Code", "Name", "min_adult_age", "Sex"]).unstack()

# copy  "both" values into columns for each sex
ref[("low", "F")] = ref[("low", "F")].fillna(ref[("low", "B")])
ref[("low", "M")] = ref[("low", "M")].fillna(ref[("low", "B")])
ref[("high", "F")] = ref[("high", "F")].fillna(ref[("high", "B")])
ref[("high", "M")] = ref[("high", "M")].fillna(ref[("high", "B")])

ref.drop([("high", "B"), ("low", "B")], axis=1, inplace=True)
ref.reset_index(inplace=True)

ref.columns = ref.columns.map("_".join)
ref.rename(
    columns={"Code_": "test", "Name_": "name", "min_adult_age_": "min_adult_age"},
    inplace=True,
)
ref["max_adult_age"] = 120
ref = ref[
    ["test", "min_adult_age", "max_adult_age", "low_F", "low_M", "high_F", "high_M"]
]
ref.to_csv("north_devon_reference_ranges.csv", index=False)
