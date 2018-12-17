title Process destination setup scripts and update od results and co-location table for study regions
echo  Process destination setup scripts and update od results and co-location table for study regions

FOR %%A  IN (%*) DO (
  python 14_recompile_destinations.py %%A
  python 15_od_distances_closest_in_study_region.py %%A
  python 16_od_count_in_buffer_distance.py %%A
  python 17_aos_co-locations.py %%A
  python 19_neighbourhood_indicators.py %%A
)

@pause







