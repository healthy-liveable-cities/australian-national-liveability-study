title Process scripts 11 to 18 for study regions
echo  Process scripts 11 to 18 for study regions

FOR %%A  IN (%*) DO (
  python 11_import_osm_and_edges_to_db.py %%A
  python 12_setup_schools.py %%A
  python 13_aos_setup.py %%A
  python 14_recompile_destinations.py %%A
  python 15_od_distances_closest_in_study_region.py %%A
  python 16_od_count_in_buffer_distance.py %%A
  python 17_aos_co-locations.py %%A
  python 18_od_aos.py %%A
)

@pause
