title Process scripts 19,20 for study regions
echo  Process scripts 19,20 for study regions

FOR %%A  IN (%*) DO (
  python 19_neighbourhood_indicators.py %%A
  python 20_parcel_exclusion.py %%A
  python 21_parcel_indicators.py %%A
)

@pause
