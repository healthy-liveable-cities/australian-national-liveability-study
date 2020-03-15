# Script:  30_pedshed.py
# Purpose: Calculate 400m pedshed for HighLife project, using previously calculated 400m service area
# Carl Higgs 20190919


import time
import psycopg2 
from sqlalchemy import create_engine
from script_running_log import script_running_log

from _project_setup import *

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db), 
                       use_native_hstore=False)

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Calculate 400m pedshed for HighLife project, using previously calculated 400m service area'
# schema where point indicator output tables will be stored
schema = point_schema

# Output databases
distance = 400
nh_sausagebuffer_summary = "nh{}m".format(distance)

# Create sausage buffer spatial index
print("Creating sausage buffer spatial index... "),
sql = '''
CREATE INDEX IF NOT EXISTS {table}_gix ON {schema}.{table} USING GIST (geom);
'''.format(table = nh_sausagebuffer_summary,
           schema = schema)
engine.execute(sql)
print("Done.")

print("Calculate pedshed")
query = '''
DROP TABLE IF EXISTS {schema}.euclidean_{distance}m;
CREATE TABLE {schema}.euclidean_{distance}m AS
SELECT p.{points_id},
       p.geom,
       ST_Area(geom) AS area_sqkm
FROM 
(SELECT {points_id}, ST_Buffer(geom,{distance}) AS geom 
   FROM {sample_point_feature}) p;

DROP TABLE IF EXISTS {schema}.pedshed_{distance}m;
CREATE TABLE {schema}.pedshed_{distance}m AS
SELECT {points_id},
       e.area_sqkm AS euclidean_{distance}m_sqkm,
       s.area_sqkm AS nh{distance}m_sqkm,
       s.area_sqkm / e.area_sqkm AS pedshed_{distance}m
FROM {schema}.euclidean_{distance}m e 
LEFT JOIN {schema}.nh{distance}m s USING ({points_id});
'''.format(schema=schema,sample_point_feature = sample_point_feature, distance=distance, points_id = points_id)
engine.execute(query)

# output to completion log    
script_running_log(script, task, start, locale)

# clean up
engine.dispose()