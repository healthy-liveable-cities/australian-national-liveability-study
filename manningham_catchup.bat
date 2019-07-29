title Manningham catchup
echo  Manningham catchup

python grant_query.py %*

FOR %%A  IN (%*) DO (
    python 02_road_network_setup.py %%A 
    python 03_hex_grid_setup.py %%A 
    python 04_create_meshblock_dwellings.py %%A 
    python 05_parcel_dwellings_setup.py %%A 
    python 06_count_parcels_in_hexes.py %%A 
    python 07_create_sausage_buffers.py %%A 
    python 08_area_linkage_tables_australia.py %%A 
    python 08_inclusion_area_geometries.py %%A 
    python 09_dwelling_density.py %%A 
    python 10_street_connectivity.py %%A 
    python 11_import_osm_and_edges_to_db.py %%A 
    python 12_setup_schools.py %%A 
    python 13_aos_setup.py %%A 
    python 14_recompile_destinations.py %%A 
    python 15_od_distances_3200m.py %%A 
    python 16_od_distances_closest_in_study_region.py %%A 
    python 17_aos_co-locations.py %%A 
    python 18_od_aos.py %%A 
    python 19_neighbourhood_indicators.py %%A 
    python 20_parcel_exclusion.py %%A 
    python 21_parcel_indicators.py %%A 
    python 22_data_checking.py %%A 
    python 23_diagnostics.py %%A 
    python 24_si_mix.py %%A 
    python 25_live_sa1_work_sa3.py %%A 
    python 26_uli.py %%A 
    python 27_area_indicators.py %%A 
)

@pause



