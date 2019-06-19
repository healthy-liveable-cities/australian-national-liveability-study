title June 2019 new exclusions
echo  June 2019 new exclusions

python grant_query.py %*
FOR %%A  IN (%*) DO (
  python 20_parcel_exclusion.py %%A
  python 21_parcel_indicators.py %%A
  python 22_data_checking.py %%A
)

@pause
