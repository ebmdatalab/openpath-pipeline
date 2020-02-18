# File locations

'Open Path April 2018 - Copy.xlsx'
'Open Path August 2018 - Copy.xlsx'
'Open Path December 2018 - Copy.xlsx'
'Open Path February 2019 - Copy.xlsx'
'Open Path January 2019 - Copy.xlsx'
'Open Path July 2018 - Copy.xlsx'
'Open Path June 2018 - Copy.xlsx'
'Open Path March 2019 - Copy.xlsx'
'Open Path May 2018 - Copy.xlsx'
'Open Path November 2018 - Copy.xlsx'
'Open Path October 2018 - Copy.xlsx'
'Open Path September 2018 - Copy.xlsx'


# File format

A single file (July 2018 in this case) contains 615k rows

The columns are:

```
Specimen_Number_Discipline                           Haem/Chem
Date_Request_Made                          2018-07-01 00:00:00
Time_Request_Made                                         1201
Patients_Number                                      P01234567
Specimen_Comment                  Sample received centrifuged.
Specimen_Number                                  CH01234/0123S
Specimen_Type_Code                                           B
Specimen_Type_Desc                                       Blood
Requesting_Organisation_Code                            L01234
Requesting_Organisation_Desc        (10) Special Place Surgery
Age_on_Date_Request_Rec'd                                  52y
Sex                                                          M
Date_Specimen_Collected                    2018-06-28 00:00:00
Date_Specimen_Received                     2018-07-01 00:00:00
Requested_Test_Code                                      BONE1
Test_Performed                                             ALP
Date_Test_Performed                        2018-07-01 00:00:00
Test_Result                                                 61
Test_Result_Range                                            N
Test_Result_Units                                         iu/L
Date_Approved                              2018-07-01 00:00:00
```

The within/outside range indicator `Test_Result_Range`, and is `H`, `L`, `N`, or null.

The test carried out is `Test_Performed`.

`Date_Request_Made` always has a value, whereas `Date_Specimen_Collected` does not, so we'll use the former

The `Age_on_Date_Request_Rec'd` field is of the form `56y` for adults, or `11y 10m` for children.

The extract is exclusively a `Specimen_Number_Discipline` of `Haem/Chem`.

# Practice codes

`Requesting_Organisation_Code` is generally of the form `L01234` or `Y01234`, but there are some exceptions, for example:

* Compass House Medical Centre (!337) (1 test)
* Parkill Medical Pract (!L83130) (1 test)
* Exminster Surgery (154) (1600 tests) - a branch of Westbank Practice (L83041)
* Morchard Bishop Surgery (19) (2200 tests) - part of Mid Devon Medical Practice (L83023)
* Wonford Walk-in Centre (203) (1000 tests) - not primary care
* Exwick Health Centre (65) (9000 tests) - part of St Thomas Medical Group (L83016)
* Colyton Health Centre(77) (800 tests) - part of Seaton and Colyton (L83007)
* Student Health Centre (82) (860 tests) -  part of St Thomas Medical Group (L83016)
* Cherlton Fitzpaine Surgery (9) (49 tests) - part of Mid Devon Medical Practice (L83023)
* Belvedere (BELV) (100 tests) - mental health

There are a bynch of hospitals in there, I think with the word
"Hospital" in the `Requesting_Organisation_Desc` field.

With Rich's local knowledge, we've made a mapping file `exeter_practices_branch.csv`

# Anonymising the dataframe

Run `python make_extract.py <filename>`

# Questions

* Is there a mapping file for branches?
* Is there any indicator for haemolised samples?
