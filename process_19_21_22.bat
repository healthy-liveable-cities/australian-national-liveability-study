title Process scripts 19,21,22 for study regions
echo  Process scripts 19,21,22 for study regions

FOR %%A  IN (%*) DO (
  python 19_neighbourhood_indicators.py %%A
  python 21_parcel_indicators.py %%A
  python 22_data_checking.py %%A
)

@pause
