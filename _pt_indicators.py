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

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Calculate post hoc public transport measure distances'

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
pt_oid_data = '{}_oid'.format(pt_points)
print("Data: {pt_oid_data}".format(pt_oid_data=pt_oid_data))
if not engine.has_table(pt_oid_data):
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
                            name = pt_oid_data)
    print(command)
    sp.call(command, shell=True)

# Create PT measures (distance, which can later be considered with regard to thresholds)
table = 'ind_pt_2019_distance_800m_cl'
print(" - {table}".format(table = table))

# Create PT measures if not existing
datasets = ['gtfs_20191008_20191205_all_headway_oid']
pt_of_interest = {"pt_any"             :"headway IS NOT NULL",
                 "pt_mode_bus"         :"mode ='bus'",
                 "pt_mode_tram"        :"mode ='tram'",
                 "pt_mode_train"       :"mode ='train'",
                 "pt_mode_ferry"       :"mode ='ferry'",
                 "pt_h60min"           :"headway <=60",
                 "pt_h30min"           :"headway <=30",
                 "pt_h25min"           :"headway <=25",
                 "pt_h20min"           :"headway <=20",
                 "pt_h10min"           :"headway <=10",
                 "pt_mode_bus_h30min"  :"mode = 'bus' AND headway <=30",
                 "pt_mode_train_h15min":"mode = 'train' AND headway <=15"}
# Construct SQL queries to return minimum distance for each query, where met
# we do this using the sorted dictionary, formatted as a list, so as to 
# retain sensible column order in final output table
queries = ',\n'.join(['MIN(CASE WHEN {} THEN distance END) {}'.format(q[1],q[0]) for q in list(sorted(pt_of_interest.items()))])

sql = '''
DROP TABLE IF EXISTS {table};
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
    LEFT JOIN {pt_oid_data} pt ON (obj->>'fid')::int = pt.objectid
    WHERE attributes!='{curly_o}{curly_c}'::jsonb
    ) o USING ({points_id})
GROUP BY {points_id};
CREATE UNIQUE INDEX {table}_idx ON {table} ({points_id});
'''.format(points_id = points_id, 
           table = table,
           queries = queries,
           curly_o = '{',
           curly_c = '}',
           pt_oid_data = pt_oid_data)
engine.execute(sql)

table = 'ind_pt_2019_headway_800m'
print(" - {table}".format(table = table))
#  The formula for effective headway within 800m is based on
#  http://ngtsip.pbworks.com/w/page/12503387/Headway%20-%20Frequency
#  supplied by Chris de Gruyter, and which presented formula
#  SUM(60/headway)/60 
#  however, this formula does not result in the estimate they present for
#  their example
#  "(60 minutes / 10 minute headway) + (60 minutes / 7 minute headway) + (60 minutes /5 minute headway)
#     = 26.6 buses/hour ... 26.6 buses hour / 60 minutes = 2.25 effective headway"
#  to achieve an effective headway of 2.25 from these values, you must do, 
#  60/26.6 = 2.25, ie. NOT 26.6/60 , which = 0.44
#  Also note that this is a rate, and the value '60' could just as easily be '720' as '1'
#  The following are all equal to 2.25806451612903225808 , or 2.26
#  (the difference from 2.25 is due to rounding error in the published formula's initial sum)
#  SELECT 60/(60/10.0 + 60/7.0 + 60/5.0)  ;
#  SELECT 720/(720/10.0 + 720/7.0 + 720/5.0)  ;
#  SELECT 1/(1/10.0 + 1/7.0 + 1/5.0)      ;
#  For simplicity, we present this in its reduced form '1'
#  So, the result presented is achieved using following formula
sql = '''
DROP TABLE IF EXISTS {table};
CREATE TABLE IF NOT EXISTS {table} AS
SELECT
{points_id},
COUNT(*) stops_800m,
MIN(headway) min_headway_800m,
MAX(headway) max_headway_800m,
AVG(headway) mean_headway_800m,
stddev_pop(headway) sd_headway_800m,
1/SUM(1/headway) effective_headway_800m
FROM parcel_dwellings p
LEFT JOIN
(SELECT {points_id},
        (obj->>'fid')::int AS fid,
        (obj->>'distance')::int AS distance,
        headway,
        mode
  FROM od_pt_800m_cl,
       jsonb_array_elements(attributes) obj
  LEFT JOIN {pt_oid_data} pt ON (obj->>'fid')::int = pt.objectid
  WHERE attributes!='{curly_o}{curly_c}'::jsonb
  ) o USING ({points_id})
WHERE distance <= 800
GROUP BY {points_id};
CREATE UNIQUE INDEX {table}_idx ON {table} ({points_id});
'''.format(points_id = points_id, 
        table = table,
        queries = queries,
        curly_o = '{',
        curly_c = '}',
        pt_oid_data = pt_oid_data)
            
engine.execute(sql)

# output to completion log    
script_running_log(script, task, start, locale)
engine.dispose()
