# Script:  16_neighbourhood_indicators.py
# Purpose: Compile destinations results and neighbourhood indicator tables
# Author:  Carl Higgs 
# Date:    20190412

import os
import sys
import time
import psycopg2 

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

print('''
This script will create a number of destination indicator tables, 
which can later be drawn on in other scripts, or used as final 
outputs.

But, please note: numerous clauses to only create tables if they 
do not already exist have been added.  If tables are wanted to be 
modified or recreated, some additional tweaking in script or 
interactively (eg to manually drop the table) will be required.

All good? Great - go!
''')

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

print("Re-import areas to ensure proper indexing, and restrict other imported areas to study region.")
# Check if the table main_mb_2016_aust_full exists; if it does, these areas have previously been re-imported, so no need to re-do
curs.execute('''SELECT 1 WHERE to_regclass('public.main_mb_2016_aust_full') IS NOT NULL;''')
res = curs.fetchone()
if res is None:
  for area in areas:
    print('{}: '.format(areas[area]['name_f'])), 
    command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" -a_srs "EPSG:{srid}" '.format(srid = srid) \
              + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
              + 'user={user} password = {pwd}" '.format(user = db_user,pwd = db_pwd) \
              + '{shp} '.format(shp = areas[area]['data']) \
              + '-lco geometry_name="geom"  -lco precision=NO ' \
              + '-nlt MULTIPOLYGON' 
    # print(command)
    sp.call(command, shell=True) 
    curs.execute('''
    DELETE FROM  {area} a 
          USING {buffered_study_region} b 
      WHERE NOT ST_Intersects(a.geom,b.geom) 
             OR a.geom IS NULL;
    '''.format(area = areas[area]['table'],
               buffered_study_region = buffered_study_region))
    conn.commit()
else:
  print('''It appears that area linkage tables have previously been imported; nice one.\n''')

print("Create area level destination counts... ")
# We drop these tables first, since some destinations may have been processed since previously running.
# These queries are quick to run, so not much cost to drop and create again.
for area in areas:
  area_name = areas[area]['name_s']
  print("{}... ".format(areas[area]['name_f'])),
  query = '''
  DROP TABLE IF EXISTS {area_name}_dest_counts;
  CREATE TABLE IF NOT EXISTS {area_name}_dest_counts AS
  SELECT a.{area_id}, dest_class, count(d.geom) AS count
  FROM {area_table} a
  LEFT JOIN 
       study_destinations d ON st_contains(a.geom,d.geom)
  GROUP BY a.{area_id},dest_class
  ORDER BY a.{area_id},dest_class;  
  '''.format(area_name = area_name,
             area_table = areas[area]['table'],
             area_id = areas[area]['id'])
  # print(query)
  curs.execute(query)
  conn.commit()
  print("Done.")

# print("Create ISO37120 indicator (hard threshold is native version; soft threshold is novel...")
# to do... could base on the nh_inds with specific thresholds

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
