title June 2019 new exclusions
echo  June 2019 new exclusions

python grant_query.py %*
FOR %%A  IN (%*) DO (
  python 01_study_region_setup.py %%A
  python 08_inclusion_area_geometries.py %%A
  python 20_parcel_exclusion.py %%A
  python 21_parcel_indicators.py %%A
  python 22_data_checking.py %%A
  python 23_diagnostics.py %%A
  python 24_area_indicators.py %%A
)

@pause
