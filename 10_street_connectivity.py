# Script:  StreetConnectivity.py
# Purpose: This script calculates StreetConnectivity (3 plus leg intersections per km2)
#          It outputs PFI, 3 legIntersections, and street connectivity to an SQL database.
#          Buffer area is referenced in SQL table nh1600m
# Author:  Carl Higgs

# import arcpy
import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import time
import os
import sys
import psycopg2
import numpy as np
from progressor import progressor

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = "Calculate StreetConnectivity (3 plus leg intersections per  km2)"

# INPUT PARAMETERS

# output tables
intersections_table = "clean_intersections_12m"
sausage_buffer_table = "sausagebuffer_{}".format(distance)
nh_sausagebuffer_summary = "nh{}m".format(distance)
street_connectivity_table = "sc_nh{}m".format(distance)


#  Size of tuple chunk sent to postgresql 
sqlChunkify = 1000

createTable_sc = '''
  CREATE TABLE IF NOT EXISTS {0}
  ({1} varchar PRIMARY KEY,
   intersection_count integer NOT NULL,
   area_sqkm double precision NOT NULL,
   sc_nh1600m double precision NOT NULL 
  ); 
  '''.format(street_connectivity_table,points_id.lower())

sc_query_A = '''
INSERT INTO {sc_table} ({id},intersection_count,area_sqkm,sc_nh1600m)
(SELECT  a.gnaf_pid, 
        COALESCE(COUNT(c.*),0) AS intersection_count,
        area_sqkm, 
        COALESCE(COUNT(c.*),0)/area_sqkm AS sc_nh1600mm
FROM {nh_area} a 
LEFT JOIN {nh_geom} b ON a.gnaf_pid = b.gnaf_pid
LEFT JOIN {intersections} c ON ST_Intersects(b.geom, c.geom)
WHERE a.{id} IN 
'''.format(sc_table = street_connectivity_table,
           id = points_id.lower(),
           nh_area = nh_sausagebuffer_summary,
           nh_geom = sausage_buffer_table,
           intersections = intersections_table)

sc_query_C = '''
  GROUP BY a.{},area_sqkm) ON CONFLICT DO NOTHING;
  '''.format(points_id.lower())

# OUTPUT PROCESS
# Connect to postgreSQL server
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

print('''Ensure previous results have been marked as 'incorrect'.
That is, check that a tables 'od_aos_incorrect','od_aos_jsonb_incorrect' and 'od_aos_progress_incorrect' all exist; if not these tables will be marked as incorrect; if so, then we proceed assuming that any od_aos tables already existing are the new amended versions''')
curs.execute('''SELECT 1 WHERE to_regclass('public.clean_intersections_15m') IS NOT NULL;''')
res = curs.fetchone()
if res is None:
  for table in ['clean_intersections', 'sc_nh1600m']:
    mark_15m_sql = '''ALTER TABLE IF EXISTS {table} RENAME TO {table}_15m;'''.format(table = table)
    print("Executing: {}... ".format(mark_15m_sql)),
    curs.execute(mark_15m_sql)
    conn.commit()
    print("Done.")
  curs.execute('''SELECT 1 WHERE to_regclass('public.clean_intersections_15m') IS NOT NULL;''')
  res = curs.fetchone()
  if res is None:
    print("At least one of these tables still does not exist, which implies at least one wasn't calculated in the first instance; or they have been manually renamed to something else, or deleted. So, we create an empty 'incorrect' table just to mark that we have checked no incorrect results persist unmarked as being incorrect")
    for table in ['clean_intersections', 'sc_nh1600m']:
      create_marked_table = '''CREATE TABLE IF NOT EXISTS {table}_incorrect AS SELECT NULL;'''.format(table = table)
      print("Executing: {}... ".format(create_marked_table)),
      curs.execute(create_marked_table)
      conn.commit()
      print("Done.")

# Check if intersections table exists, and if not import intersections
curs.execute("select exists(select * from information_schema.tables where table_name=%s)", (intersections_table,))
if curs.fetchone()[0] is False:
  print("Copy cleaned intersections from gpkg to postgis..."),
  command = (
          ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
          ' PG:"host={host} port=5432 dbname={db}'
          ' user={user} password = {pwd}" '
          ' {gpkg} "{feature}" '
          ' -nln {intersections} '
          ' -lco geometry_name="geom"'.format(host = db_host,
                                       db = db,
                                       user = db_user,
                                       pwd = db_pwd,
                                       gpkg = os.path.basename(clean_intersections_gpkg),
                                       feature = clean_intersections_locale,
                                       intersections = intersections_table) 
          )
  print(command)
  sp.call(command, shell=True,cwd=os.path.dirname(os.path.join(folderPath,clean_intersections_gpkg)))
  print("Done (although, if it didn't work you can use the printed command above to do it manually)")

# Now calculate street connectivity (three way intersections w/ in nh1600m/area in  km2)
curs.execute("select exists(select * from information_schema.tables where table_name=%s)", (street_connectivity_table,))
if curs.fetchone()[0] is False:
  print("Create table {}... ".format(street_connectivity_table)),
  subTaskStart = time.time()
  curs.execute(createTable_sc)
  conn.commit()
  print(" Done.")
  
print("Fetch list of processed parcels, if any... "), 
# Checks id numbers from sausage buffers against
antijoin = '''
   SELECT {id} 
   FROM {nh_geom} nh 
   WHERE NOT EXISTS
   (SELECT 1 FROM {sc_table} sc 
    WHERE sc.{id} = nh.{id});
'''.format(id = points_id.lower(),
           nh_geom  = sausage_buffer_table,
           sc_table = street_connectivity_table)
curs.execute(antijoin)
point_id_list = [x[0] for x in  list(curs)]
print("Done.")

denom = len(point_id_list)
if denom != 0:
  print("Processing points...")
  count = 0
  chunkedPoints = list()
  for point in point_id_list:
    count += 1
    chunkedPoints.append(point) 
    if (count % sqlChunkify == 0) :
        curs.execute('{} ({}) {}'.format(sc_query_A,','.join("'"+x+"'" for x in chunkedPoints),sc_query_C))
        conn.commit()
        chunkedPoints = list()
        progressor(count,denom,start,"{}/{} points processed".format(count,denom))
  if(count % sqlChunkify != 0):
     curs.execute('{} ({}) {}'.format(sc_query_A,','.join("'"+x+"'" for x in chunkedPoints),sc_query_C))
     conn.commit()
  
  progressor(count,denom,start,"{}/{} points processed".format(count,denom))

if denom == 0:
  print("All points have been processed (ie. all point ids from the sausage buffer table are now records in the street connectivity table.")
  
# output to completion log    
script_running_log(script, task, start)

# clean up
conn.close()

