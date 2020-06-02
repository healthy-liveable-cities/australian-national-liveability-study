title Process 2018/19 Liveability Score Cards (January 2020)
echo  Process 2018/19 Liveability Score Cards (January 2020)

FOR %%A  IN (%*) DO (
  python _pt_indicators.py %%A
  python 23_collate_indicator_tables.py %%A
  python 25_area_indicators.py %%A
  python _export_indicators.py %%A
)

@pause




