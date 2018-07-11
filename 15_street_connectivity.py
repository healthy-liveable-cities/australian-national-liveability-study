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

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = "Calculate StreetConnectivity (3 plus leg intersections per  km2)"

# INPUT PARAMETERS

## specify locations
points =  'parcel_dwellings'
# denominator = int(arcpy.GetCount_management(points).getOutput(0))

#  Copy the intersections from gdb to postgis, correcting the projection in process
command = ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' \
       + ' PG:"host={host} port=5432 dbname={db}'.format(host = db_host,db = db) \
       + ' user={user} password = {pwd}" '.format(user = db_user,pwd = db_pwd) \
       + ' {gdb} "{feature}" '.format(gdb = os.path.dirname(intersections),feature = os.path.basename(intersections)) \
       + ' -lco geometry_name="geom"' \
       + ' -nln "intersections_3plus" '
sp.call(command, shell=True)

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()
curs.execute(grant_query)
conn.commit()
conn.close()


fields = [points_id]


# output tables
intersections_table = "intersections_3plus"
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

# Now calculate street connectivity (three way intersections w/ in nh1600m/area in  km2)
print("create table {}... ".format(street_connectivity_table)),
subTaskStart = time.time()
curs.execute(createTable_sc)
conn.commit()
print(" Done."))	
  
print("fetch list of processed parcels, if any..."), 
# (for string match to work, had to select first item of returned tuple)
curs.execute("SELECT {} FROM {}".format(points_id.lower(),sausage_buffer_table))
raw_point_id_list = list(curs)
raw_point_id_list = [x[0] for x in raw_point_id_list]

curs.execute("SELECT {} FROM {}".format(points_id.lower(),street_connectivity_table))
completed_points = list(curs)
completed_points = [x[0] for x in completed_points]

point_id_list = [x for x in raw_point_id_list if x not in completed_points]  
print(" Done.")

denom = len(point_id_list)
count = 0
chunkedPoints = list()

print("Processing points...")
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

# output to completion log    
script_running_log(script, task, start)

# clean up
conn.close()

