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

## PROOF OF CONCEPT
# sql = '''
# DROP TABLE IF EXISTS test_freq_pt_20200209;
# CREATE TABLE test_freq_pt_20200209 AS
# SELECT DISTINCT ON (gnaf_pid)
       # gnaf_pid,
       # distance,
       # mode,
       # headway,
       # p.geom
# FROM 
# parcel_dwellings p
# LEFT JOIN
# (SELECT gnaf_pid,
        # (obj->>'fid')::int AS fid,
        # (obj->>'distance')::int AS distance
# FROM od_pt_800m_cl,
    # jsonb_array_elements(attributes) obj
# WHERE attributes!='{}'::jsonb) o USING(gnaf_pid)
# LEFT JOIN gtfs_20191008_20191205_all_headway_oid pt ON o.fid = pt.objectid
# WHERE pt.headway <= 20
  # AND o.distance <=400
# ORDER BY gnaf_pid, distance;
# '''

# engine.execute(sql)

sql = '''
DROP TABLE IF EXISTS ind_pt_2019;
CREATE TABLE ind_pt_2019 AS
SELECT parcel_dwellings.gnaf_pid,
       (filtered.distance IS NOT NULL)::int AS pt_regular_20mins_in_400m,
       filtered.distance,
       filtered.headway,
       filtered.mode,
       parcel_dwellings.geom
FROM 
parcel_dwellings
LEFT JOIN
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
    AND o.distance <=400
  ORDER BY gnaf_pid, distance) filtered 
ON parcel_dwellings.gnaf_pid = filtered.gnaf_pid;
'''

engine.execute(sql)
