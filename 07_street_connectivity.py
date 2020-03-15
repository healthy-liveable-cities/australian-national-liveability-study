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
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = "Calculate StreetConnectivity (3 plus leg intersections per  km2)"

# INPUT PARAMETERS

# schema where point indicator output tables will be stored
schema = point_schema

intersections_table = "network.clean_intersections_12m"
nh_area = "nh{}m".format(distance)

table = "sc_nh{}m".format(distance)

#  Size of tuple chunk sent to postgresql 
sqlChunkify = 1000


# Connect to postgreSQL server
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

# Now calculate street connectivity (three way intersections w/ in nh1600m/area in  km2)
sql = '''
  CREATE TABLE IF NOT EXISTS {schema}.{table}
  ({id} {type} PRIMARY KEY,
   intersection_count integer NOT NULL,
   area_sqkm double precision NOT NULL,
   sc_nh1600m double precision NOT NULL 
  ); 
  '''.format(table = table,
             schema=schema,
             id = points_id.lower(),
             type = points_id_type)
curs.execute(sql)
conn.commit()
  
print("Fetch list of processed parcels, if any... "), 
# Checks id numbers from sausage buffers against
antijoin = '''
   SELECT {id}::text
   FROM {schema}.{nh_area} nh 
   WHERE NOT EXISTS
   (SELECT 1 FROM {schema}.{sc_table} sc 
    WHERE sc.{id} = nh.{id});
'''.format(id = points_id.lower(),
           schema=schema,
           nh_area  = nh_area,
           sc_table = table)
curs.execute(antijoin)
point_id_list = [x[0] for x in  list(curs)]
print("Done.")

sc_query_A = '''
INSERT INTO {schema}.{table} ({id},intersection_count,area_sqkm,sc_nh1600m)
(SELECT  a.{id}, 
        COALESCE(COUNT(c.*),0) AS intersection_count,
        area_sqkm, 
        COALESCE(COUNT(c.*),0)/area_sqkm AS sc_nh1600mm
FROM {schema}.{nh_area} a 
LEFT JOIN {intersections} c ON ST_Intersects(a.geom, c.geom)
WHERE a.{id} IN 
'''.format(table = table,
           schema=schema,
           id = points_id.lower(),
           nh_area = nh_area,
           intersections = intersections_table)

sc_query_C = '''
  GROUP BY a.{},area_sqkm) ON CONFLICT DO NOTHING;
  '''.format(points_id.lower())

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

