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
WHERE p.amenity IN ('school','college','university') 
   OR p.landuse IN ('school','college','university');
'''.format(osm_prefix = osm_prefix),
'''
ALTER TABLE osm_schools ADD COLUMN area_ha double precision; 
UPDATE osm_schools SET area_ha = ST_Area(geom)/10000.0;
ALTER TABLE osm_schools ADD COLUMN is_school boolean; 
UPDATE osm_schools SET is_school = TRUE;
''',
'''
ALTER TABLE osm_schools ADD COLUMN school_tags jsonb;       
ALTER TABLE osm_schools ADD COLUMN school_count int;       
UPDATE osm_schools o SET school_count = 0;
''',
'''
UPDATE osm_schools t1 
   SET school_tags = jsonb(t2.school_tags), school_count = t1.school_count + t2.school_count
FROM (-- here we aggregate and count the sets of school tags associated with school polygons
      -- by virtue of those being for their associated schools their closest match within 100m.
      -- School points more than 100m from a polygon will remain unmatched.
      -- The basis for an allowed distance of 100m is that some points may be located at the driveway
      -- from which a school is accessed --- for some schools, this may be several hundred metres 
      -- from a road.
      -- We haven't restricted it to polygons with null as it may be that 
      -- previously associated polgon-school groupings (by virtue of intersection, 0 distance)
      -- may have a further school point coincidentally (mis?)geocoded outside the bounds of the 
      -- OSM school polygon.  If there is no better matchin school polygon, it may be best to assume 
      -- that the point should best be associated with the other schools.
      SELECT osm_id,
             count(*) AS school_count,
             jsonb_agg(to_jsonb(t) - 'osm_id'::text  - 'matched_school'::text  - 'school_tags'::text)  AS school_tags
      FROM (SELECT DISTINCT ON ({school_id})
            CASE 
              WHEN ST_Intersects(schools.geom, osm.geom) THEN 0
              ELSE ST_Distance(schools.geom, ST_ExteriorRing(osm.geom))::int  
            END AS dist,
            schools.*, 
            osm.osm_id,
            osm.school_tags
            FROM (SELECT a.*, o.matched_school FROM {ext_schools} a 
                  LEFT JOIN 
                  (SELECT (jsonb_array_elements(school_tags)->>'{school_id}') AS matched_school FROM osm_schools) o 
                  ON a.{school_id}::text = o.matched_school,{studyregion} s 
                  WHERE ST_Intersects(a.geom,s.geom) AND matched_school IS NULL) schools,
                  osm_schools osm
      WHERE ST_DWithin(schools.geom, ST_ExteriorRing(osm.geom), 150) OR ST_Intersects(schools.geom, osm.geom)
      ORDER BY {school_id},ST_Distance(schools.geom, ST_ExteriorRing(osm.geom))) t
      GROUP BY osm_id) t2
 WHERE t1.osm_id = t2.osm_id;
-- -- School match table - commented out for now, but useful for cross checking
-- DROP TABLE IF EXISTS school_matches;    
-- CREATE TABLE school_matches AS 
-- SELECT a.*, o.matched_school FROM {ext_schools} a 
--                   LEFT JOIN 
--                   (SELECT (jsonb_array_elements(school_tags)->>'{school_id}') AS matched_school FROM osm_schools) o 
--                   ON a.{school_id}::text = o.matched_school,{studyregion} s 
--                   WHERE ST_Intersects(a.geom,s.geom);
'''.format(ext_schools =  os.path.basename(school_destinations),
           school_id = school_id.lower(),
           studyregion = buffered_study_region)
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
