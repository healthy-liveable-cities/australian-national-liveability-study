title Process neighbourhood measures scripts for study regions
echo Process neighbourhood measures scripts for study regions

FOR %%A  IN (%*) DO (
  python 15_area_linkage_tables.py %%A
  python 16_dwelling_density.py %%A
  python 17_street_connectivity.py %%A
)

@pause


