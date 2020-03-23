title update and backup indicators
echo  update and backup indicators

python grant_query.py %*
FOR %%A  IN (%*) DO (
  python 23_collate_indicator_tables.py %%A
  python 25_area_indicators.py %%A
  python 27_export_map_summaries.py %%A
  python 27_export_parcel_summaries.py %%A
)

@pause






