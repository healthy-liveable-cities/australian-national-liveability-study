title Process OSM, AOS and destination setup scripts for study regions
echo  Process OSM, AOS and destination setup scripts for study regions

FOR %%A  IN (%*) DO (
  python 11_import_osm_and_edges_to_db.py %%A
  python 12_setup_schools.py %%A
  python 13_aos_setup.py %%A
  python 14_recompile_destinations.py %%A
)

@pause




