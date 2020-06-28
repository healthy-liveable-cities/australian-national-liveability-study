title June 2020 ABS area indicator and observatory dump including full distance public open space
echo  June 2020 ABS area indicator and observatory dump including full distance public open space

FOR %%A  IN (%*) DO (
  python 11_ABS_indicators.py.py %%A
  python 27_area_indicators.py %%A
  python 28_policy_indicators.py %%A
  python 29_export_map_summaries.py %%A
  python 30_export_parcel_summaries.py %%A
)

@pause
