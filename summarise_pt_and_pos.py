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

sql = [['''SELECT (SUM(ST_Area(geom_public))+SUM(ST_Area(geom_water)))/10000 AS area_ha FROM open_space_areas;''','POS area','Ha']  ,
       ['''SELECT COUNT(*) FROM study_destinations WHERE dest_class = 'gtfs_2018_stop_30_mins_final';''','PT (30 min freq)','stops'],
       ['''SELECT COUNT(*) FROM study_destinations WHERE dest_class = 'gtfs_2018_stops';''',             'PT (any)','stops'],
       ['''SELECT COUNT(*) FROM study_destinations WHERE dest_class = 'gtfs_2018_stops_bus';''',         'PT (bus)','stops'],
       ['''SELECT COUNT(*) FROM study_destinations WHERE dest_class = 'gtfs_2018_stops_train';''',       'PT (train)','stops'],
       ['''SELECT COUNT(*) FROM study_destinations WHERE dest_class = 'gtfs_2018_stops_ferry';''',       'PT (ferry)','stops']]

for query in sql:
  curs.execute(query[0])
  print("{}: {} {}".format(query[1],list(curs)[0][0],query[2]))

conn.close()
