title Disability and health indicator output
echo  Disability and health indicator output

python grant_query.py %*
FOR %%A  IN (%*) DO (
  python 24_area_indicators.py %%A
  python disability_health_inds_sa1_dwellings.py %%A
)

@pause
