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
sql = '''
DROP TABLE IF EXISTS ind_pt_2019;
CREATE TABLE ind_pt_2019 AS
-- in the final table, we select those results 
-- closer than 400m
SELECT parcel_dwellings.gnaf_pid,
       (filtered_20.distance <= 400)::int AS pt_regular_20mins_in_400m,
       (filtered_25.distance <= 400)::int AS pt_regular_25mins_in_400m,
       parcel_dwellings.geom
FROM 
parcel_dwellings
LEFT JOIN
-- in the inner table we select the shortest distance
-- to a transport stop with average service frequency (headway)
-- of 20 mins or less
(SELECT DISTINCT ON (gnaf_pid)
       p.gnaf_pid,
       o.distance,
       pt.mode,
       pt.headway
  FROM 
  parcel_dwellings p
  LEFT JOIN
  (SELECT gnaf_pid,
             (obj->>'fid')::int AS fid,
             (obj->>'distance')::int AS distance
     FROM od_pt_800m_cl,
         jsonb_array_elements(attributes) obj
     WHERE attributes!='{}'::jsonb) o ON p.gnaf_pid = o.gnaf_pid
  LEFT JOIN gtfs_20191008_20191205_all_headway_oid pt ON o.fid = pt.objectid
  WHERE pt.headway <= 20
  ORDER BY gnaf_pid, distance) filtered_20
ON parcel_dwellings.gnaf_pid = filtered_20.gnaf_pid
LEFT JOIN
-- in the inner table we select the shortest distance
-- to a transport stop with average service frequency (headway)
-- of 20 mins or less
(SELECT DISTINCT ON (gnaf_pid)
       p.gnaf_pid,
       o.distance,
       pt.mode,
       pt.headway
  FROM 
  parcel_dwellings p
  LEFT JOIN
  (SELECT gnaf_pid,
             (obj->>'fid')::int AS fid,
             (obj->>'distance')::int AS distance
     FROM od_pt_800m_cl,
         jsonb_array_elements(attributes) obj
     WHERE attributes!='{}'::jsonb) o ON p.gnaf_pid = o.gnaf_pid
  LEFT JOIN gtfs_20191008_20191205_all_headway_oid pt ON o.fid = pt.objectid
  WHERE pt.headway <= 25
  ORDER BY gnaf_pid, distance) filtered_25
ON parcel_dwellings.gnaf_pid = filtered_25.gnaf_pid;
'''

engine.execute(sql)

# # Create PT measures (distances, which can later be considered with regard to thresholds)
# # In addition to public open space (pos), also includes sport areas and blue space
# table = ['ind_pt_2019_distances_800m_cl','pt']
# print(" - {table}".format(table = table[0])),

# sql = '''
# CREATE TABLE IF NOT EXISTS {table} AS SELECT {id} FROM parcel_dwellings;
# '''.format(table = table[0], id = points_id.lower())
# curs.execute(sql)
# conn.commit()

# # Create PT measures if not existing
# datasets = ['gtfs_20191008_20191205_all_headway_oid','']
# pt_of_interest = [["pt_any"         ,"pt.headway IS NOT NULL"],
                  # ["pt_bus"         ,"pt.mode ='bus'"],
                  # ["pt_tram"        ,"pt.mode ='tram'"],
                  # ["pt_train"       ,"pt.mode ='train'"],
                  # ["pt_ferry"       ,"pt.mode ='ferry'"],
                  # ["pt_h20min"      ,"pt.headway <=20"],
                  # ["pt_h25min"      ,"pt.headway <=25"],
                  # ["pt_h30min"      ,"pt.headway <=30"],
                  # ["pt_bus_h30min"  ,"pt.mode = 'bus' AND pt.headway <=30"],
                  # ["pt_train_h15min","pt.mode = 'train' AND pt.headway <=15"]]
# for pt in pt_of_interest:
    # measure = '{}_d_800m_cl'.format(pt[0])
    # where = pt[1]
    # sql = '''
    # SELECT column_name 
    # FROM information_schema.columns 
    # WHERE table_name='{table}' and column_name='{column}';
    # '''.format(table = table[0],column = measure)
    # curs.execute(sql)
    # res = curs.fetchone()
    # if not res:   
        # add_and_update_measure = '''
        # DROP INDEX IF EXISTS {table}_idx;
        # ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {measure} int[];
        # UPDATE {table} t 
        # SET {measure} = pt_filtered.distances
        # FROM parcel_dwellings orig
        # LEFT JOIN 
        # (SELECT p.{id}, 
                # array_agg(distance) AS distances
           # FROM parcel_dwellings p
           # LEFT JOIN
              # (SELECT gnaf_pid,
                         # (obj->>'fid')::int AS fid,
                         # (obj->>'distance')::int AS distance
                 # FROM od_pt_800m_cl,
                     # jsonb_array_elements(attributes) obj
                 # WHERE attributes!='{}'::jsonb) o ON p.gnaf_pid = o.gnaf_pid
           # LEFT JOIN {data} pt ON o.fid = pt.objectid
           # WHERE pt.headway <= 25
           # GROUP BY p{id}) pt_filtered ON orig.{id} = pt_filtered.{id}
            # WHERE t.{id} = orig.{id};
        # '''.format(id = points_id, table = table[0], data = data,measure = measure,where = where)
        # curs.execute(add_and_update_measure)
        # conn.commit()
    # print("."),
