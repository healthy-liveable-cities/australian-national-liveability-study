title Process some preliminary scripts for study regions
echo Process some preliminary scripts for study regions

FOR %%A  IN (%*) DO (
  python 01_study_region_setup.py %%A
  python 02_road_network_setup.py %%A
  python 03_hex_grid_setup.py %%A
  python 04_create_meshblock_dwellings.py %%A
  python 05_parcel_dwellings_setup.py %%A
  python 06_count_parcels_in_hexes.py %%A
)

@pause
