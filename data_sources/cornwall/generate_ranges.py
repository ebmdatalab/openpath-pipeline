import pandas as pd

ref = pd.read_csv("metadata/TestFormatRanges.csv")

ref.rename(columns={"TestFormatCode": "test"}, inplace=True)

# filter to those which are "Active" otherwise there are duplicates
ref = ref.loc[ref["ActiveRange"] == True]

# filter out any age ranges for infants given in months, weeks or days
# (note that some are blank, i.e. apply to all ages)
ref = ref.loc[(ref["AgeUnits"] == "Y") | pd.isnull(ref["AgeUnits"])]

# filter out any rows with "conditional values" - not sure what these
# are but they tend to have same values as rows without them
ref = ref.loc[(pd.isnull(ref["ConditionalCode"]))]


# replace blanks in ages and sexes with "all"
ref.loc[
    pd.isnull(ref["Age"]), "Age"
] = 120  # using 120 for consistency with their convention
ref.loc[pd.isnull(ref["Sex"]), "Sex"] = "B"  # (both)
ref.loc[pd.isnull(ref["AgeUnits"]), "AgeUnits"] = "Y"  # (both)

# fill blank dates with a valid but very old date, convert all to
# datetime
ref["DateValidFrom"] = ref["DateValidFrom"].fillna("1901-01-01")
ref["DateValidFrom"] = pd.to_datetime(ref["DateValidFrom"])

# rank dates for each test, sex and age:
ref["daterank"] = ref.groupby(["test", "Sex", "Age"])["DateValidFrom"].rank(
    ascending=False
)

# select only the latest range for each test, sex and age
ref = ref.loc[ref["daterank"] == 1]

# Check for remaining duplicates
check = ref.groupby(["test", "Age", "Sex"])["FlagRangeLow", "FlagRangeHigh"].nunique()
assert len(check.loc[(check["FlagRangeLow"] > 1) | (check["FlagRangeHigh"] > 1)]) == 0

## Split B (both sexes) into separate rows for M and F

# group and rename columns
ref = (
    ref.groupby(["test", "Age", "Sex"])["FlagRangeLow", "FlagRangeHigh"].max().unstack()
)
ref = ref.rename(columns={"FlagRangeLow": "low", "FlagRangeHigh": "high"}).reset_index()

# copy  "both" values into columns for each individual sex
ref[("low", "F")] = ref[("low", "F")].fillna(ref[("low", "B")])
ref[("low", "M")] = ref[("low", "M")].fillna(ref[("low", "B")])
ref[("high", "F")] = ref[("high", "F")].fillna(ref[("high", "B")])
ref[("high", "M")] = ref[("high", "M")].fillna(ref[("high", "B")])

# drop the "both sexes" columns
ref.drop([("high", "B"), ("low", "B")], axis=1, inplace=True)

# rearrange data
ref = ref.set_index(["test", "Age"]).stack().reset_index()


## Find lower limit for each age band

# File supplied only included upper limits, e.g. "18", "45" "120" but
# we need to know at what lower age limit each of these apply.

# Find rank of each age limit for each test and sex
# e.g. "18":1, "45":2, "120":3
ref["agerank"] = ref.groupby(["test", "Sex"])["Age"].rank()
ref["agerankR"] = ref.groupby(["test", "Sex"])["Age"].rank(ascending=False)

# for all tests, cross-join all age limits to make all combinations
# e.g. (18,18),(18,45),(18,120),(45,120),(120,120) and so on
# then limit to sequential age combinations only
#  where rank1 is 1 less than rank2, e.g. (1,2), (2,3)
#  or where there is only one range
ref = ref.copy()
ref = ref.merge(ref[["test", "Sex", "Age", "agerank"]], on=(["test", "Sex"]))
ref = ref.loc[
    (ref["agerank_y"] == ref["agerank_x"] - 1)
    | (ref["agerank_x"] + ref["agerankR"] == 2)
]

# in the case where there is only one range we need to replace the lower limit with 0
ref.loc[(ref["agerank_x"] + ref["agerankR"] == 2), "Age_y"] = 0

ref.rename(columns={"Age_x": "max_adult_age", "Age_y": "min_adult_age"}, inplace=True)

# drop ref ranges for children, and unnecessary columns
ref = ref.loc[ref["max_adult_age"] > 18].drop(
    ["agerank_x", "agerank_y", "agerankR"], axis=1
)


# rearrange data to match required format
ref = ref.set_index(["test", "min_adult_age", "max_adult_age", "Sex"]).unstack()
ref.columns = ref.columns.map("_".join)
ref = ref.reset_index()[
    ["test", "min_adult_age", "max_adult_age", "low_F", "low_M", "high_F", "high_M"]
]

ref.to_csv("cornwall_ref_ranges.csv", index=False)
