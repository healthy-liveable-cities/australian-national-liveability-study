title November 2019 Destination update - indicator calculation
echo  November 2019 Destination update - indicator calculation

python _ad_hoc_create_distance_schema.py %*
FOR %%A  IN (%*) DO (
  python _ad_hoc_19_neighbourhood_indicators.py %%A
)

@pause
