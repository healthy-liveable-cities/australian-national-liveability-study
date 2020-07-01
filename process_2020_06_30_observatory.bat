title June 2020 fix up bugs noted for June indicator dump
echo  June 2020 fix up bugs noted for June indicator dump

FOR %%A  IN (%*) DO (
  python _ad_hoc_coallesced_threhsold_functions.py %%A
  python 11_ABS_indicators.py %%A
  python 25_collate_indicator_tables.py %%A
  python 26_data_checking.py %%A
  python 27_area_indicators.py %%A
  python 28_policy_indicators.py %%A
  python 29_export_map_summaries.py %%A
  python 30_export_parcel_summaries.py %%A

)

@pause
