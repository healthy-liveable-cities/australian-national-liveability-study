# Purpose: Summarise counts of transport stops and public OS areas for study regions
#
# Authors:  Carl Higgs, Julianna Rozek
# Date:    20181213

import psycopg2

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

sql = [['''SELECT ROUND(((SUM(ST_Area(geom_public))+SUM(ST_Area(geom_water)))/10000.0)::numeric,2) AS area_ha FROM open_space_areas;''','POS area','Ha']  ,
       ['''SELECT COUNT(*) FROM study_destinations WHERE dest_class = 'gtfs_2018_stop_30_mins_final';''','PT (30 min freq)','stops'],
       ['''SELECT COUNT(*) FROM study_destinations WHERE dest_class = 'gtfs_2018_stops';''',             'PT (any)','stops'],
       ['''SELECT COUNT(*) FROM study_destinations WHERE dest_class = 'gtfs_2018_stops_bus';''',         'PT (bus)','stops'],
       ['''SELECT COUNT(*) FROM study_destinations WHERE dest_class = 'gtfs_2018_stops_train';''',       'PT (train)','stops'],
       ['''SELECT COUNT(*) FROM study_destinations WHERE dest_class = 'gtfs_2018_stops_ferry';''',       'PT (ferry)','stops']]
   
values = [locale]
for query in sql:
  curs.execute(query[0])
  values.append(list(curs)[0][0])

print("Pretty:")  
print('''{:25} {:>16} {:>16} {:>16} {:>16} {:>16} {:>16}'''.format('locale','POS area (Ha)','PT (30 min freq)','PT (any)','PT (bus)','PT (train)','PT (ferry)'))
print('''{:25}{:16} {:16} {:16} {:16} {:16} {:16}'''.format(*values))
    
print("\nComma seperated:")        
print('''{},{},{},{},{},{},{}'''.format('locale','POS area (Ha)','PT (30 min freq)','PT (any)','PT (bus)','PT (train)','PT (ferry)'))
print('''{},{},{},{},{},{},{}'''.format(*values))

conn.close()


