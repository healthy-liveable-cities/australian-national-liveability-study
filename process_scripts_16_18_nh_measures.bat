title Process neighbourhood measures scripts for study regions
echo Process neighbourhood measures scripts for study regions

FOR %%A  IN (%*) DO (
  python 16_area_linkage_tables.py %%A
  python 17_dwelling_density.py %%A
  python 18_street_connectivity.py %%A
)

@pause


