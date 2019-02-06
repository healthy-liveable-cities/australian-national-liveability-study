title Process neighbourhood measures scripts for study regions
echo Process neighbourhood measures scripts for study regions

FOR %%A  IN (%*) DO (
  python 08_area_linkage_tables.py %%A
  python 09_dwelling_density.py %%A
  python 10_street_connectivity.py %%A
)

@pause


