# Purpose: Prepare Areas of Open Space (AOS) for ntnl liveability indicators
#           -- *** Assumes already in correct projection for project (e.g. GDA2020 GA LCC) *** 
#           -- copies features within study region to project gdb
#           -- calculates geodesic area in hectares
#           -- makes temporary line feature from polygons
#           -- traces vertices at set interval (aos_vertices in config file) -- pseudo entry points
#           -- creates three subset features of AOS pseudo-entries, at intervals of 20, 30 and 50m from road network
#           -- Preliminary EDA suggests the 30m distance pseudo entry points will be most appropriate to use 
#              for OD network analysis
#
#         This assumes 
#           -- a study region specific section of OSM has been prepared and is referenced in the setup xlsx file
#           -- the postgis_sfcgal extension has been created in the active database
#
# Author:  Carl Higgs
# Date:    20180626


import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import arcpy
import time
import psycopg2
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Prepare Areas of Open Space (AOS)'

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

print("Copy the school destinations from gdb to postgis..."),
command = (
        ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
        ' PG:"host={host} port=5432 dbname={db}'
        ' user={user} password = {pwd}" '
        ' {gdb} "{feature}" '
        ' -lco geometry_name="geom"'.format(host = db_host,
                                     db = db,
                                     user = db_user,
                                     pwd = db_pwd,
                                     gdb = os.path.join(folderPath,dest_dir,src_destinations),
                                     feature = school_destinations) 
        )
print(command)
sp.call(command, shell=True)
print("Done (although, if it didn't work you can use the printed command above to do it manually)")



aos_setup = [''' 
-- Create table for OSM school polygons
DROP TABLE IF EXISTS osm_schools;
CREATE TABLE osm_schools AS 
SELECT * FROM {osm_prefix}_polygon p 
WHERE p.amenity IN ('school','college') OR p.landuse IN ('school');
'''.format(osm_prefix = osm_prefix),
'''
ALTER TABLE osm_schools ADD COLUMN area_ha double precision; 
UPDATE osm_schools SET area_ha = ST_Area(geom)/10000.0;
ALTER TABLE osm_schools ADD COLUMN is_school boolean; 
UPDATE osm_schools SET is_school = TRUE;
''',
'''
ALTER TABLE osm_schools ADD COLUMN ext_school_id int; 
ALTER TABLE osm_schools ADD COLUMN ext_school_dist int; 
''',
'''
UPDATE osm_schools o 
   SET ext_school_id = {school_id}::int, ext_school_dist = 0
  FROM {ext_schools} s
 WHERE ST_Intersects(s.geom, o.geom);
'''.format(ext_schools =  os.path.basename(school_destinations),
           school_id = school_id.lower()),
'''
UPDATE osm_schools o 
   SET ext_school_id = {school_id}::int, ext_school_dist = dist::int
  FROM (SELECT DISTINCT ON ({school_id}) 
          {school_id}, 
           s.osm_id, 
           ST_Distance(s.geom, o.geom)  as dist
        FROM {ext_schools} AS o , osm_schools AS s  
        WHERE ST_DWithin(s.geom, o.geom, 500) 
        ORDER BY {school_id}, s.osm_id, ST_Distance(s.geom, o.geom)) t
 WHERE o.osm_id = t.osm_id 
   AND o.ext_school_dist IS NULL;
'''.format(ext_schools =  os.path.basename(school_destinations),
           school_id = school_id.lower())
]


for sql in aos_setup:
    start = time.time()
    print("\nExecuting: {}".format(sql))
    curs.execute(sql)
    conn.commit()
    print("Executed in {} mins".format((time.time()-start)/60))
 
 
# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
