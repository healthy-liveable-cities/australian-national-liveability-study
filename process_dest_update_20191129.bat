title November 2019 Destination update
echo  November 2019 Destination update

python grant_query.py %*
FOR %%A  IN (%*) DO (
  python 14_recompile_destinations.py %%A
  python _ad_hoc_15_od_distances_3200m.py %%A
)
FOR %%A  IN (%*) DO (
  python 14_recompile_destinations.py %%A
  python _ad_hoc_15_od_distances_3200m.py %%A
)

@pause
