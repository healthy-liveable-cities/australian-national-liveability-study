title January 2020 Destination update - indicator calculation
echo  January 2020 Destination update - indicator calculation

FOR %%A  IN (%*) DO (
  python python 14_recompile_destinations.py %%A
  python python 15_od_distances_3200m_cl.py %%A
)

@pause
