title June 2019 data check
echo  June 2019 data check

python grant_query.py %*
FOR %%A  IN (%*) DO (
  python _area_linkage_tables_check.py %%A
  python _nh_inds_check.py %%A
  python 20_parcel_exclusion.py %%A
  python 21_parcel_indicators.py %%A
  python 22_data_checking.py %%A
)

@pause
