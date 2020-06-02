# Script:  who.py
# Purpose: who is processing which study regions?
# Author:  Carl Higgs
# Date:    20181009

# Import custom variables for National Liveability indicator process
# import modules
import os
import sys
import pandas
from sqlalchemy import create_engine

# Load settings from _project_configuration.xlsx
xls = pandas.ExcelFile(os.path.join(sys.path[0],'_project_configuration.xlsx'))
df_about = pandas.read_excel(xls, 'about')
print(df_about.iloc[0].reset_index()['index'].to_string(index=False).encode('utf'))
df_parameters = pandas.read_excel(xls, 'parameters',index_col=0)
df_studyregion = pandas.read_excel(xls, 'study_regions',index_col=1)
responsible = df_studyregion['responsible']
year   = df_parameters.loc['year']['value']
# SQL Settings
db_host   = df_parameters.loc['db_host']['value'].encode('utf')
db_port   = '{}'.format(df_parameters.loc['db_port']['value'])
db_user   = df_parameters.loc['db_user']['value'].encode('utf')
db_pwd    = df_parameters.loc['db_pwd']['value'].encode('utf')

locales = responsible[responsible == sys.argv[1]].sort_values().index.values.tolist()
for locale in locales:
  print("\n{}".format(locale))
  db = 'li_{}_{}'.format(locale,year)
  engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))
  print("Auditing script_log to make sure script name sequence is up to date as of 29 November 2018... "),
  connection = engine.connect()
  sql = ['''UPDATE script_log SET script = '00_create_database.py' WHERE script LIKE '%%create_database%%' AND script != '00_create_database.py';''',
         '''UPDATE script_log SET script = '01_study_region_setup.py' WHERE script LIKE '%%study_region_setup%%' AND script != '01_study_region_setup.py';''',
         '''UPDATE script_log SET script = '02_road_network_setup.py' WHERE script LIKE '%%road_network_setup%%' AND script != '02_road_network_setup.py';''',
         '''UPDATE script_log SET script = '03_hex_grid_setup.py' WHERE script LIKE '%%hex_grid_setup%%' AND script != '03_hex_grid_setup.py';''',
         '''UPDATE script_log SET script = '04_create_meshblock_dwellings.py' WHERE script LIKE '%%create_meshblock_dwellings%%' AND script != '04_create_meshblock_dwellings.py';''',
         '''UPDATE script_log SET script = '05_parcel_dwellings_setup.py' WHERE script LIKE '%%parcel_dwellings_setup%%' AND script != '05_parcel_dwellings_setup.py';''',
         '''UPDATE script_log SET script = '06_count_parcels_in_hexes.py' WHERE script LIKE '%%count_parcels_in_hexes%%' AND script != '06_count_parcels_in_hexes.py';''',
         '''UPDATE script_log SET script = '07_create_sausage_buffers.py' WHERE script LIKE '%%create_sausage_buffers%%' AND script != '07_create_sausage_buffers.py';''',
         '''UPDATE script_log SET script = '08_area_linkage_tables.py' WHERE script LIKE '%%area_linkage_tables%%' AND script != '08_area_linkage_tables.py';''',
         '''UPDATE script_log SET script = '09_dwelling_density.py' WHERE script LIKE '%%dwelling_density%%' AND script != '09_dwelling_density.py';''',
         '''UPDATE script_log SET script = '10_street_connectivity.py' WHERE script LIKE '%%street_connectivity%%' AND script != '10_street_connectivity.py';''',
         '''UPDATE script_log SET script = '11_import_osm_and_edges_to_db.py' WHERE script LIKE '%%import_osm_and_edges_to_db%%' AND script != '11_import_osm_and_edges_to_db.py';''',
         '''UPDATE script_log SET script = '12_setup_schools.py' WHERE script LIKE '%%setup_schools%%' AND script != '12_setup_schools.py';''',
         '''UPDATE script_log SET script = '13_aos_setup.py' WHERE script LIKE '%%aos_setup%%' AND script != '13_aos_setup.py';''',
         '''UPDATE script_log SET script = '14_recompile_destinations.py' WHERE script LIKE '%%recompile_destinations%%' AND script != '14_recompile_destinations.py';''',
         '''UPDATE script_log SET script = '15_od_distances_closest_in_study_region.py' WHERE script LIKE '%%od_distances_closest_in_study_region%%' AND script != '15_od_distances_closest_in_study_region.py';''',
         '''UPDATE script_log SET script = '16_od_count_in_buffer_distance.py' WHERE script LIKE '%%od_count_in_buffer_distance%%' AND script != '16_od_count_in_buffer_distance.py';''',
         '''UPDATE script_log SET script = '17_aos_co-locations.py' WHERE script LIKE '%%aos_co-locations%%' AND script != '17_aos_co-locations.py';''',
         '''UPDATE script_log SET script = '18_od_aos.py' WHERE script LIKE '%%od_aos%%' AND script != '18_od_aos.py';''',
         '''UPDATE script_log SET script = '19_neighbourhood_indicators.py' WHERE script LIKE '%%neighbourhood_indicators%%' AND script != '19_neighbourhood_indicators.py';''',
         '''UPDATE script_log SET script = '20_parcel_indicators.py' WHERE script LIKE '%%parcel_indicators%%' AND script != '20_parcel_indicators.py';''',
         '''UPDATE script_log SET script = '21_parcel_exclusion.py' WHERE script LIKE '%%parcel_exclusion%%' AND script != '21_parcel_exclusion.py';''',
         '''UPDATE script_log SET script = '22_urban_liveability_index.py' WHERE script LIKE '%%urban_liveability_index%%' AND script != '22_urban_liveability_index.py';''',
         '''UPDATE script_log SET script = '23_area_indicators.py' WHERE script LIKE '%%area_indicators%%' AND script != '23_area_indicators.py';''',
         '''UPDATE script_log SET script = 'testing_melb_pos : 13b_pos_setup_testing_vpa_foi.py' WHERE script LIKE '%%ting_melb_pos : 13b_pos_setup_testing_vpa_foi%%' AND script != 'testing_melb_pos : 13b_pos_setup_testing_vpa_foi.py';''',
         '''UPDATE script_log SET script = 'testing_melb_pos : 13c_pos_setup_testing_vpa_foi_add_osm2.py' WHERE script LIKE '%%_pos_setup_testing_vpa_foi_add_osm2%%' AND script != 'testing_melb_pos : 13c_pos_setup_testing_vpa_foi_add_osm2.py';''',
         '''UPDATE script_log SET script = 'testing_melb_pos : 15b_od_aos_testing_melb_vpa.py' WHERE script LIKE '%%_od_aos_testing_melb_vpa%%' AND script != 'testing_melb_pos : 15b_od_aos_testing_melb_vpa.py';''',
         '''UPDATE script_log SET script = 'testing_melb_pos : 15c_aos_foi_osm_vpa_comparison.py' WHERE script LIKE '%%_aos_foi_osm_vpa_comparison%%' AND script != 'testing_melb_pos : 15c_aos_foi_osm_vpa_comparison.py';''']
  for query in sql:
    connection.execute(query)
  print("Done.")
  print('\nScripts processed for {}:'.format(locale))
  status = pandas.read_sql("SELECT script,datetime_completed FROM script_log", engine)
  print(status)
