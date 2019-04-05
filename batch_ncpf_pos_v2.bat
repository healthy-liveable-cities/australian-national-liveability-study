title Recompiled destinations and summarise for study regions
echo  Recompiled destinations and summarise for study regions
echo  **NOTE: Assumes you have a current version of destinations database**

FOR %%A  IN (%*) DO (
python ncpf_od_closest_pos_v2.py %%A any
python ncpf_od_closest_pos_v2.py %%A large
)

@pause
