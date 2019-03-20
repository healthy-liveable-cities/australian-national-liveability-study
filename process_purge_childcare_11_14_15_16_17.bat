title Purge destinations (childcare and redundant NHSD), rename Integrated Family Services feature, re-import destinations and process destination setup scripts and update od results and co-location table for study regions
echo  Purge destinations (childcare and redundant NHSD), rename Integrated Family Services feature, re-import destinations and process destination setup scripts and update od results and co-location table for study regions

FOR %%A  IN (%*) DO (
  python purge_destinations.py %%A
  python 11_import_osm_and_edges_to_db.py %%A
  python 14_recompile_destinations.py %%A
  python 15_od_distances_3200m.py %%A
  python 16_od_distances_closest_in_study_region.py %%A
  python 17_aos_co-locations.py %%A
)

@pause
