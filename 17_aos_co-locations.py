# Purpose: Evaluate Euclidean buffer co-location of Areas of Open Space with other amenities; in particular
#          -- cafes and restaurants @ 100m
#          -- lighting @ 0m?  (subject to data availability!)
#                   - check osm tag in first instance
#                   - then check external data sources
#          -- toilets @ 100m
# Author:  Carl Higgs
# Date:    20180626


# import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
# import arcpy
# import time
# import psycopg2
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Co-locate Areas of Open Space (AOS) with other amenities'

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
# conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
# curs = conn.cursor()  

print('''This is a place holder scripts for Areas of Open Space (AOS) co-location.
#
# Purpose: Evaluate Euclidean buffer co-location of Areas of Open Space with other amenities; in particular
#          -- cafes and restaurants @ 100m
#          -- lighting @ 0m?  (subject to data availability!)
#                   - check osm tag in first instance
#                   - then check external data sources
#          -- toilets @ 100m

So, yet to be written - watch this space...


Draft code:
ALTER TABLE open_space_areas ADD COLUMN co_location_100m jsonb;
UPDATE open_space_areas o 
   SET co_location_100m  = t.co_location_100m
  FROM (SELECT aos_id, jsonb_agg(dest_class) AS co_location_100m
          FROM open_space_areas osa, study_destinations d 
         WHERE ST_DWithin(osa.geom,d.geom,100) 
            OR ST_Intersects(osa.geom,d.geom)
         GROUP BY aos_id) t 
  WHERE o.aos_id = t.aos_id
    AND t.co_location_100m IS NOT NULL;

    
SELECT aos_id,
       jsonb_pretty(attributes) AS attributes,
       jsonb_pretty(co_location_100m) AS co_location_100m,
       co_location_100m ? 'cafe_osm' AS near_cafe,
       co_location_100m ?| ARRAY['cafe_osm','restaurant_osm','pub_osm'] AS near_casual_eatery,
       co_location_100m ?| ARRAY(SELECT dest_name FROM dest_type WHERE domain LIKE '%Community, Culture and Leisure%') AS near_community_culture
 FROM open_space_areas o;

    
    
select aos_id,  
       jsonb_pretty(co_location_100m) AS co_location_100m, 
       co_location_100m ?| ARRAY['cafe_osm','restaurant_osm','pub_osm'] AS has_casual_eatery
       co_location_100m ?| ARRAY['cafe_osm','restaurant_osm','pub_osm'] AS has_casual_eatery
from open_space_areas

''')

