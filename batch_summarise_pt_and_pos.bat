title Recompiled destinations and summarise for study regions
echo  Recompiled destinations and summarise for study regions
echo  **NOTE: Assumes you have a current version of destinations database**

FOR %%A  IN (%*) DO (
  python 14_recompile_destinations.py %%A
  python summarise_pt_and_pos.py %%A
)

@pause
