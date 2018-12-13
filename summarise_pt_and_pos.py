# Purpose: Summarise counts of transport stops and public OS areas for study regions
#
# Authors:  Carl Higgs, Julianna Rozek
# Date:    20181213

import os
import psycopg2
from datetime import date

today = str(date.today())
# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

sql = ['''
       SELECT SUM(ST_Area(ST_Intersection(a.geom_public,b.geom)))/10000.0
       FROM aos_public_osm a, {} b;
       '''.format(study_region),
       '''
       SELECT COUNT(*)
       FROM
       (SELECT DISTINCT ON (s.geom)
       1
        FROM study_destinations s, gccsa_2018 b 
       WHERE dest_class = 'gtfs_2018_stop_30_mins_final' 
         AND ST_Intersects(s.geom,b.geom)
      GROUP BY s.geom) t;
       '''.format(study_region),
       '''
       SELECT COUNT(*)
       FROM
       (SELECT DISTINCT ON (s.geom)
       1
        FROM study_destinations s, gccsa_2018 b 
       WHERE dest_class = 'gtfs_2018_stops' 
         AND ST_Intersects(s.geom,b.geom)
      GROUP BY s.geom) t;
       '''.format(study_region),
       '''
       SELECT COUNT(*)
       FROM
       (SELECT DISTINCT ON (s.geom)
       1
        FROM study_destinations s, gccsa_2018 b 
       WHERE dest_class = 'gtfs_2018_stops_bus' 
         AND ST_Intersects(s.geom,b.geom)
      GROUP BY s.geom) t;
       '''.format(study_region),
       '''
       SELECT COUNT(*)
       FROM
       (SELECT DISTINCT ON (s.geom)
       1
        FROM study_destinations s, gccsa_2018 b 
       WHERE dest_class = 'gtfs_2018_stops_train' 
         AND ST_Intersects(s.geom,b.geom)
      GROUP BY s.geom) t;
       '''.format(study_region),
       '''
       SELECT COUNT(*)
       FROM
       (SELECT DISTINCT ON (s.geom)
       1
        FROM study_destinations s, gccsa_2018 b 
       WHERE dest_class = 'gtfs_2018_stops_tram' 
         AND ST_Intersects(s.geom,b.geom)
      GROUP BY s.geom) t;
       '''.format(study_region),
       '''
       SELECT COUNT(*)
       FROM
       (SELECT DISTINCT ON (s.geom)
       1
        FROM study_destinations s, gccsa_2018 b 
       WHERE dest_class = 'gtfs_2018_stops_ferry' 
         AND ST_Intersects(s.geom,b.geom)
      GROUP BY s.geom) t;
       '''.format(study_region)]

values = [locale]
for query in sql:
  curs.execute(query)
  values.append(list(curs)[0][0])
  
conn.close()

output = os.path.join(folderPath,'pt_pos_summary_{}_{}.csv'.format(responsible[locale],today))
header = '''{},{},{},{},{},{},{},{}\n'''.format('locale','POS area (Ha)','PT (30 min freq)','PT (any)','PT (bus)','PT (train)','PT (tram)','PT (ferry)')
if not os.path.exists(output):
   with open(output, "w") as f:
     f.write(header)
     print('''{:25} {:>16} {:>16} {:>16} {:>16} {:>16} {:>16} {:>16}'''.format('locale','POS area (Ha)','PT (30 min freq)','PT (any)','PT (bus)','PT (train)','PT (tram)','PT (ferry)'))

with open(output, "a") as f:
  f.write('''{},{},{},{},{},{},{},{}\n'''.format(*values))
  print('''{:25}{:16} {:16} {:16} {:16} {:16} {:16} {:>16}'''.format(*values))


