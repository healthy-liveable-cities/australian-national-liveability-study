title November 2019 Destination update - indicator calculation
echo  November 2019 Destination update - indicator calculation

FOR %%A  IN (%*) DO (
  python _ad_hoc_19_neighbourhood_indicators.py %%A
  python _ad_hoc_final_inds.py %%A
  python __ad_hoc_export_indicators.py %%A
)

@pause
