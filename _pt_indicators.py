# Script:  _pt_indicators.py
# Purpose: Calculate post hoc PT indicators
# Authors: Carl Higgs
# Date: 2020-02-08

import os
import time
import sys
import psycopg2 
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db), 
                       use_native_hstore=False)
# Due to idiosyncracy of ArcPy gdb, when data was originally copied to this,
# it dropped the fid field and called it 'OBJECTID'; this was not noticed until
# post analysis, until which point I believed it had been renamed but retained the 
# same values as fid.  Hence, the data is recorded with 'fid' in the attributes
# This is misleading though, and 'fid' in the attributes of the _od_pt_800m_cl table
# actually relates to the region specific OBJECTID, not the national datasets 'fid'

# Hence, we require the following work around
# LATER, UPDATE SCRIPTS SO THAT fid IS USED IF POSSIBLE, 
# OR AT LEAST THAT THE RECORDED FIELD IS CALLED objectid
# so its match is clear
if not engine.has_table('{}_oid'.format(pt_points)):
    print("\nCopy PT data to postgis, with locale arcpy oids..."),
    command = (
            ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
            ' PG:"host={host} port=5432 dbname={db}'
            ' user={user} password = {pwd}" '
            ' {gdb} {feature} '
            ' -lco geometry_name="geom" '
            ' -nln "{name}" '
            ).format(host = db_host,
                            db = db,
                            user = db_user,
                            pwd = db_pwd,
                            gdb = gdb_path,
                            feature = pt_points,
                            name = '{}_oid'.format(pt_points))
    print(command)
    sp.call(command, shell=True)

# Calculate PT indicators
# sql = '''
# DROP TABLE IF EXISTS ind_pt_2019;
# CREATE TABLE ind_pt_2019 AS
# -- in the final table, we select those results 
# -- closer than 400m
# SELECT parcel_dwellings.gnaf_pid,
       # (filtered_20.distance <= 400)::int AS pt_regular_20mins_in_400m,
       # (filtered_25.distance <= 400)::int AS pt_regular_25mins_in_400m,
       # parcel_dwellings.geom
# FROM 
# parcel_dwellings
# LEFT JOIN
# -- in the inner table we select the shortest distance
# -- to a transport stop with average service frequency (headway)
# -- of 20 mins or less
# (SELECT DISTINCT ON (gnaf_pid)
       # p.gnaf_pid,
       # o.distance,
       # pt.mode,
       # pt.headway
  # FROM 
  # parcel_dwellings p
  # LEFT JOIN
  # (SELECT gnaf_pid,
             # (obj->>'fid')::int AS fid,
             # (obj->>'distance')::int AS distance
     # FROM od_pt_800m_cl,
         # jsonb_array_elements(attributes) obj
     # WHERE attributes!='{}'::jsonb) o ON p.gnaf_pid = o.gnaf_pid
  # LEFT JOIN gtfs_20191008_20191205_all_headway_oid pt ON o.fid = pt.objectid
  # WHERE pt.headway <= 20
  # ORDER BY gnaf_pid, distance) filtered_20
# ON parcel_dwellings.gnaf_pid = filtered_20.gnaf_pid
# LEFT JOIN
# -- in the inner table we select the shortest distance
# -- to a transport stop with average service frequency (headway)
# -- of 20 mins or less
# (SELECT DISTINCT ON (gnaf_pid)
       # p.gnaf_pid,
       # o.distance,
       # pt.mode,
       # pt.headway
  # FROM 
  # parcel_dwellings p
  # LEFT JOIN
  # (SELECT gnaf_pid,
             # (obj->>'fid')::int AS fid,
             # (obj->>'distance')::int AS distance
     # FROM od_pt_800m_cl,
         # jsonb_array_elements(attributes) obj
     # WHERE attributes!='{}'::jsonb) o ON p.gnaf_pid = o.gnaf_pid
  # LEFT JOIN gtfs_20191008_20191205_all_headway_oid pt ON o.fid = pt.objectid
  # WHERE pt.headway <= 25
  # ORDER BY gnaf_pid, distance) filtered_25
# ON parcel_dwellings.gnaf_pid = filtered_25.gnaf_pid;
# '''

# engine.execute(sql)


## Sketch 2 
# This creates binary indicators, however, in first instance, we want min distance
# so we can later take average distances given headway / mode
#
# SELECT
# gnaf_pid,
# COALESCE(MAX((distance <= 400 AND headway <= 25)::int),0) pt_h25min
# FROM parcel_dwellings p
# LEFT JOIN
    # (SELECT gnaf_pid,
            # (obj->>'fid')::int AS fid,
            # (obj->>'distance')::int AS distance,
            # headway,
            # mode
    # FROM od_pt_800m_cl,
        # jsonb_array_elements(attributes) obj
    # LEFT JOIN gtfs_20191008_20191205_all_headway_oid pt ON (obj->>'fid')::int = pt.objectid
    # WHERE attributes!='{}'::jsonb
    # ) o USING (gnaf_pid)
# GROUP BY gnaf_pid
# ;

# Create PT measures (distance, which can later be considered with regard to thresholds)
table = ['ind_pt_2019_distance_800m_cl','pt']
print(" - {table}".format(table = table[0])),

sql = '''
CREATE TABLE IF NOT EXISTS {table} AS SELECT {id} FROM parcel_dwellings;
'''.format(table = table[0], id = points_id.lower())
curs.execute(sql)
conn.commit()

# Create PT measures if not existing
datasets = ['gtfs_20191008_20191205_all_headway_oid','']
pt_of_interest = {"pt_any"        :"headway IS NOT NULL",
                 "pt_mode_bus"         :"mode ='bus'",
                 "pt_mode_tram"        :"mode ='tram'",
                 "pt_mode_train"       :"mode ='train'",
                 "pt_mode_ferry"       :"mode ='ferry'",
                 "pt_h60min"      :"headway <=60",
                 "pt_h30min"      :"headway <=30",
                 "pt_h25min"      :"headway <=25",
                 "pt_h20min"      :"headway <=20",
                 "pt_h10min"      :"headway <=10",
                 "pt_mode_bus_h30min"  :"mode = 'bus' AND headway <=30",
                 "pt_mode_train_h15min":"mode = 'train' AND headway <=15"}
queries = ',\n'.join(['MIN(CASE WHEN {} THEN distance END) {}'.format(q[1],q[0]) for q in list(sorted(pt_of_interest.items()))])
sql = '''
CREATE TABLE IF NOT EXISTS {table} AS
SELECT
{points_id},
{queries}
FROM parcel_dwellings p
LEFT JOIN
    (SELECT {points_id},
            (obj->>'fid')::int AS fid,
            (obj->>'distance')::int AS distance,
            headway,
            mode
    FROM od_pt_800m_cl,
        jsonb_array_elements(attributes) obj
    LEFT JOIN {data} pt ON (obj->>'fid')::int = pt.objectid
    WHERE attributes!='{}'::jsonb
    ) o USING ({points_id})
GROUP BY {points_id}
'''.format({points_id} = points_id, 
           table = table[0],
           data = data)
engine.execute(sql)
engine.dispose()
