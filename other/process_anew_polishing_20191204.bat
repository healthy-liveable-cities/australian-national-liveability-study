title November 2019 Destination update - indicator calculation
echo  November 2019 Destination update - indicator calculation

FOR %%A  IN (%*) DO (
  python 20_parcel_exclusion.py %%A
)

@pause
