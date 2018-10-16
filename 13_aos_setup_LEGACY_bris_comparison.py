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

# CUSTOM VARIABLES SPECIFIC TO THIS LEGACY ANALYSIS

# FOR THIS ANALYSIS IN PARTICULAR
# I ran the following at command prompt before running this script:
## shp2pgsql -s 4326 -W "LATIN1" D:\ntnl_li_2018_template\data\destinations\pos\bris\osm_bris_10km_raw_20180517.shp | psql -h localhost -d li_bris_2016 -U postgres
# AND THEN, in psql to make the table treated equivalently to regular script
## ALTER TABLE osm_bris_10km_raw_20180517 RENAME TO osm_bris_10km_raw_20180517_polygon;
# and then added in an hstore tags column; in regular analysis this provides additional information, but this wasn't present in source file
## ALTER TABLE osm_bris_10km_raw_20180517_polygon ADD COLUMN tags hstore;
# and also renamed 'geom' to 'way' to make way for later creation of geom as the epsg 7845 projected geometry
## ALTER TABLE osm_bris_10km_raw_20180517_polygon RENAME COLUMN geom TO way;
# and then also made sure privileges were granted
## GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO arc_sde;
## GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO arc_sde;
## GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO python;
## GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO python;
## ALTER TABLE osm_bris_10km_raw_20180517_polygon OWNER TO python;

osm_prefix = 'osm_bris_10km_raw_20180517'
db = 'li_bris_2016'
gdb_path = 'D:/ntnl_li_2018_template/data/study_region/bris/li_bris_2016.gdb'
db_sde_path = 'D:/ntnl_li_2018_template/data/study_region/bris/li_bris_2016.sde'

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

# import buffered study region OSM excerpt to pgsql, 
# OLD REGULAR BIT
##If its decided that this should only be done if not already exists, uncomment below; however, this may cause complications
##curs.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", ('{}_polygon'.format(osm_prefix),))
##if curs.fetchone()[0] is False:
##print("Copying OSM excerpt to pgsql..."),
##command = 'osm2pgsql -U {user} -l -d {db} {osm} --hstore --style {style} --prefix {prefix}'.format(user = db_user, 
##                                                                               db = db,
##                                                                               osm = osm_source,
##                                                                               style = osm2pgsql_style,
##                                                                               prefix = osm_prefix) 
##sp.call(command, shell=True, cwd=osm2pgsql_exe)                           
##print("Done.")

# Define tags for which presence of values is suggestive of some kind of open space 
# These are defined in the ind_study_region_matrix worksheet 'open_space_defs' under the 'possible_os_tags' column.
possible_os_tags = '\n'.join(['ALTER TABLE {}_polygon ADD COLUMN IF NOT EXISTS "{}" varchar;'.format(osm_prefix,x.encode('utf')) for x in df_aos["possible_os_tags"].dropna().tolist()])

os_landuse = "'{}'".format("','".join([x.encode('utf') for x in df_aos["os_landuse"].dropna().tolist()]))
os_boundary = "'{}'".format("','".join([x.encode('utf') for x in df_aos["os_boundary"].dropna().tolist()]))

specific_inclusion_criteria = '\nAND '.join(['({})'.format(x.encode('utf')) for x in df_aos["specific_inclusion_criteria"].dropna().tolist()])

water_features = ','.join(["'{}'".format(x.encode('utf')) for x in df_aos["water_tags_for_natural_landuse_leisure"].dropna().tolist()])
water_sports = ','.join(["'{}'".format(x.encode('utf')) for x in df_aos["water_sports"].dropna().tolist()])

linear_feature_criteria = '\nAND '.join(['({})'.format(x.encode('utf')) for x in df_aos["linear_feature_criteria_AND"].dropna().tolist()])

identifying_tags = ','.join(["'{}'".format(x.encode('utf')) for x in df_aos["identifying_tags_to_exclude_other_than_%name%"].dropna().tolist()])
exclude_tags_like_name = '''(SELECT array_agg(tags) from (SELECT DISTINCT(skeys(tags)) tags FROM open_space) t WHERE tags ILIKE '%name%')'''

os_add_as_tags = ',\n'.join(['"{}"'.format(x.encode('utf')) for x in df_aos["os_add_as_tags"].dropna().tolist()])


aos_setup = ['''
-- Add geom column to polygon table, appropriately transformed to project spatial reference system
ALTER TABLE {osm_prefix}_polygon ADD COLUMN geom geometry; 
UPDATE {osm_prefix}_polygon SET geom = ST_Transform(way,7845); 
'''.format(osm_prefix = osm_prefix),
'''
-- Add other columns which are important if they exists, but not important if they don't
-- --- except that there presence is required for ease of accurate querying.
{}'''.format(possible_os_tags),
'''
-- Create an 'Open Space' table
DROP TABLE IF EXISTS open_space;
CREATE TABLE open_space AS 
SELECT * FROM {osm_prefix}_polygon p 
WHERE (p.leisure IS NOT NULL 
    OR p.natural IS NOT NULL 
    OR p.sport IS NOT NULL  
    OR p.landuse IN ({os_landuse})
    OR p.boundary IN ({os_boundary})
    OR beach IS NOT NULL
    OR river IS NOT NULL
    OR water IS NOT NULL 
    OR waterway IS NOT NULL 
    OR wetland IS NOT NULL )
  AND {specific_inclusion_criteria};
'''.format(osm_prefix = osm_prefix, 
           os_landuse = os_landuse,
           os_boundary = os_boundary,
           specific_inclusion_criteria = specific_inclusion_criteria),
'''
-- Create unique POS id 
ALTER TABLE open_space ADD COLUMN os_id SERIAL PRIMARY KEY;         
-- The below line is expensive to run and may be unnecesessary, so is commented out
--CREATE INDEX open_space_idx ON open_space USING GIST (geom);
''',
'''
-- Create variable for park size 
ALTER TABLE open_space ADD COLUMN area_ha double precision; 
UPDATE open_space SET area_ha = ST_Area(geom)/10000.0;
''',
'''
 -- Create water feature indicator
ALTER TABLE open_space ADD COLUMN water_feature boolean;
UPDATE open_space SET water_feature = FALSE;
UPDATE open_space SET water_feature = TRUE 
   WHERE "natural" IN ({water_features})
      OR beach IS NOT NULL
      OR river IS NOT NULL
      OR water IS NOT NULL 
      OR waterway IS NOT NULL 
      OR wetland IS NOT NULL 
      OR landuse IN ({water_features})
      OR leisure IN ({water_features}) 
      OR sport IN ({water_sports});
'''.format(water_features = water_features,
           water_sports = water_sports),
'''
-- Create variable for AOS area excluding water
ALTER TABLE open_space ADD COLUMN water_geom geometry; 
UPDATE open_space SET water_geom = geom WHERE water_feature = TRUE;
''',
'''
-- Create variable for medial axis as a hint of linearity
-- https://postgis.net/2015/10/25/postgis_sfcgal_extension/
ALTER TABLE open_space ADD COLUMN medial_axis_length double precision; 
UPDATE open_space SET medial_axis_length = ST_Length(ST_ApproximateMedialAxis(geom));
''',
'''
-- Take ratio of approximate medial axis length (AMAL) to park area
ALTER TABLE open_space ADD COLUMN amal_to_area_ratio double precision; 
UPDATE open_space SET amal_to_area_ratio = medial_axis_length/area_ha;
''',
'''
-- get geometry of symetric difference of the convex hull of the geometry
ALTER TABLE open_space ADD COLUMN symdiff_convhull_geoms geometry; 
UPDATE open_space SET symdiff_convhull_geoms = ST_SymDifference(geom,ST_ConvexHull(geom));
''',
'''
-- get number of symetrically different shards from the convex hull
ALTER TABLE open_space ADD COLUMN num_symdiff_convhull_geoms double precision; 
UPDATE open_space SET num_symdiff_convhull_geoms = ST_NumGeometries(symdiff_convhull_geoms);
''',
'''
ALTER TABLE open_space ADD COLUMN roundness double precision; 
UPDATE open_space SET roundness = ST_Area(geom)/(ST_Area(ST_MinimumBoundingCircle(geom)));
''',
'''
-- Create indicator for linear features informed through EDA of OS topology
ALTER TABLE open_space ADD COLUMN linear_feature boolean;
UPDATE open_space SET linear_feature = FALSE;
UPDATE open_space SET linear_feature = TRUE 
WHERE {linear_feature_criteria};
'''.format(linear_feature_criteria=linear_feature_criteria),
'''
-- Create 'Acceptable Linear Feature' indicator (alf?)
ALTER TABLE open_space ADD COLUMN acceptable_linear_feature boolean;
UPDATE open_space SET acceptable_linear_feature = FALSE WHERE linear_feature = TRUE;
UPDATE open_space o SET acceptable_linear_feature = TRUE
FROM (SELECT os_id,geom FROM open_space WHERE linear_feature = FALSE) nlf
WHERE o.linear_feature IS TRUE 
 AND ST_Intersects(o.geom,nlf.geom)
 AND (st_area(st_intersection(o.geom,nlf.geom))/st_area(o.geom)) > .1
 OR (ST_Length(ST_CollectionExtract(ST_Intersection(o.geom,nlf.geom), 2)) > 50
     AND o.os_id < nlf.os_id 
     AND ST_Touches(o.geom,nlf.geom)
     AND o.medial_axis_length < 500);     
-- a feature identified as linear is acceptable as an OS if it is
--  large enough to contain an OS of sufficient size (0.4 Ha?) 
-- (suggests it may be an odd shaped park with a lake; something like that)
UPDATE open_space o SET acceptable_linear_feature = TRUE
FROM open_space alt
WHERE o.linear_feature IS TRUE      
 AND  o.acceptable_linear_feature IS FALSE    
 AND  o.geom && alt.geom 
  AND st_area(st_intersection(o.geom,alt.geom))/10000.0 > 0.4
  AND o.os_id != alt.os_id;
''',
''' 
-- Create variable for school intersection 
DROP TABLE IF EXISTS schools;
CREATE TABLE schools AS 
SELECT * FROM {osm_prefix}_polygon p 
WHERE p.amenity IN ('school','college') OR p.landuse IN ('school');
ALTER TABLE schools ADD COLUMN is_school boolean; 
UPDATE schools SET is_school = TRUE;
'''.format(osm_prefix = osm_prefix),
'''
-- Set up OS for distinction based on location within a school
ALTER TABLE open_space ADD COLUMN in_school boolean; 
UPDATE open_space SET in_school = FALSE;
UPDATE open_space SET in_school = TRUE FROM schools WHERE ST_CoveredBy(open_space.geom,schools.geom);
ALTER TABLE open_space ADD COLUMN is_school boolean; 
UPDATE open_space SET is_school = FALSE;
ALTER TABLE open_space ADD COLUMN no_school_geom geometry; 
UPDATE open_space SET no_school_geom = geom WHERE is_school = FALSE;
''',
'''
-- Insert school polygons in open space, restricting to relevant de-identified subset of tags (ie. no school names, contact details, etc)
INSERT INTO open_space (tags,is_school,geom)
SELECT  slice(tags, 
              ARRAY['amenity',
                    'designation'     ,
                    'fee'             ,
                    'grades'          ,
                    'isced'           ,
                    'school:gender'   ,
                    'school:enrolment',
                    'school:selective',
                    'school:specialty']),
        is_school,
        geom
FROM schools;
''',
'''
-- Remove potentially identifying tags from records
UPDATE open_space SET tags =  tags - {exclude_tags_like_name} - ARRAY[{identifying_tags}]
;
'''.format(exclude_tags_like_name = exclude_tags_like_name,
           identifying_tags = identifying_tags),
'''
-- Create Areas of Open Space (AOS) table
-- this includes schools and contains indicators to differentiate schools, and parks within schools
-- the 'geom' attributes is the area within an AOS not including a school
--    -- this is what we want to use to evaluate collective OS area within the AOS (aos_ha)
-- the 'geom_w_schools' attribute is the area including the school (so if there is no school, this is equal to geom)
--    -- this is what we will use to create entry nodes for the parks (as otherwise school ovals would be inaccessible)
-- School AOS features 
--    -- can always be excluded from analysis, or an analysis can be restricted to focus on these.
--    -- contains a subset of anonymised tags present for the school itself 
--    -- specifically, 'designation', 'fee', 'grades', 'isced', 'school:gender', 'school:enrolment', 'school:selective', 'school:specialty'

DROP TABLE IF EXISTS open_space_areas; 
CREATE TABLE open_space_areas AS 
WITH clusters AS(
    SELECT unnest(ST_ClusterWithin(open_space.geom, .001)) AS gc
       FROM open_space WHERE in_school IS FALSE AND (linear_feature IS FALSE OR acceptable_linear_feature IS TRUE)
    UNION
    SELECT  unnest(ST_ClusterWithin(school_os.geom, .001)) AS gc
       FROM open_space AS school_os WHERE in_school IS TRUE OR is_school IS TRUE
    UNION
    SELECT  linear_os.geom AS gc
       FROM open_space AS linear_os WHERE linear_feature IS TRUE AND acceptable_linear_feature IS FALSE
    UNION
    SELECT  linear_os.geom AS gc
       FROM open_space AS linear_os WHERE linear_feature IS TRUE AND acceptable_linear_feature IS TRUE
       )
, unclustered AS( --unpacking GeomCollections
    SELECT row_number() OVER () AS cluster_id, (ST_DUMP(gc)).geom AS geom 
       FROM clusters)
SELECT cluster_id as aos_id, 
       jsonb_agg(jsonb_strip_nulls(to_jsonb( 
           (SELECT d FROM (SELECT {os_add_as_tags}) d)) || hstore_to_jsonb(tags) )) AS attributes,
    COUNT(1) AS numgeom,
    ST_Union(no_school_geom) AS geom,
    ST_Union(water_geom) AS geom_water,
    ST_Union(geom) AS geom_w_schools
    FROM open_space
    INNER JOIN unclustered USING(geom)
    GROUP BY cluster_id;   
'''.format(os_add_as_tags = os_add_as_tags),
''' 
CREATE UNIQUE INDEX aos_idx ON open_space_areas (aos_id);  
CREATE INDEX idx_aos_jsb ON open_space_areas USING GIN (attributes);
''',
''' 
-- Create variable for park size 
ALTER TABLE open_space_areas ADD COLUMN aos_ha double precision; 
-- note aos_ha_total includes school area
ALTER TABLE open_space_areas ADD COLUMN aos_ha_total double precision; 
ALTER TABLE open_space_areas ADD COLUMN aos_ha_water double precision; 
''',
'''
-- Calculate total area of OS in Ha and where no OS is present (ie. a school without parks) set this to zero
UPDATE open_space_areas SET aos_ha = COALESCE(ST_Area(geom)/10000.0,0);
UPDATE open_space_areas SET aos_ha_water = COALESCE(ST_Area(geom_water)/10000.0,0);
''',
'''
-- Calculate total area of OS in Ha, including schools
UPDATE open_space_areas SET aos_ha_total = ST_Area(geom_w_schools)/10000.0; 
''',
'''
-- Create variable for School OS percent
ALTER TABLE open_space_areas ADD COLUMN school_os_percent numeric; 
UPDATE open_space_areas SET school_os_percent = 100 * aos_ha/aos_ha_total::numeric WHERE aos_ha!=aos_ha_total AND aos_ha_total > 0; 
''',
'''
-- Create variable for Water percent
ALTER TABLE open_space_areas ADD COLUMN water_percent numeric; 
UPDATE open_space_areas SET water_percent = 0; 
UPDATE open_space_areas SET water_percent = 100 * aos_ha_water/aos_ha_total::numeric WHERE aos_ha > 0; 
''',
'''
-- Create a linestring aos table 
-- the 'school_bounds' prereq feature un-nests the multipolygons to straight polygons, so we can take their exterior rings
DROP TABLE IF EXISTS aos_line;
CREATE TABLE aos_line AS 
WITH school_bounds AS
   (SELECT aos_id, ST_SetSRID(st_astext((ST_Dump(geom_w_schools)).geom),7845) AS geom_w_schools  FROM open_space_areas)
SELECT aos_id, ST_Length(geom_w_schools)::numeric AS length, geom_w_schools    
FROM (SELECT aos_id, ST_ExteriorRing(geom_w_schools) AS geom_w_schools FROM school_bounds) t;
''',
'''
-- Generate a point every 20m along a park outlines: 
DROP TABLE IF EXISTS aos_nodes; 
CREATE TABLE aos_nodes AS 
 WITH aos AS 
 (SELECT aos_id, 
         length, 
         generate_series(0,1,20/length) AS fraction, 
         geom_w_schools FROM aos_line) 
SELECT aos_id,
       row_number() over(PARTITION BY aos_id) AS node, 
       ST_LineInterpolatePoint(geom_w_schools, fraction)  AS geom_w_schools 
FROM aos;

CREATE INDEX aos_nodes_idx ON aos_nodes USING GIST (geom_w_schools);
ALTER TABLE aos_nodes ADD COLUMN aos_entryid varchar; 
UPDATE aos_nodes SET aos_entryid = aos_id::text || ',' || node::text; 
''',
'''
-- Preparation of reference line feature to act like roads
ALTER TABLE {osm_prefix}_line ADD COLUMN geom geometry; 
UPDATE {osm_prefix}_line SET geom = ST_Transform(way,7845); 
'''.format(osm_prefix = osm_prefix),
'''
-- Create table of points within 20m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
DROP TABLE IF EXISTS aos_nodes_20m_line;
CREATE TABLE aos_nodes_20m_line AS 
SELECT DISTINCT n.* 
FROM aos_nodes n, 
     {osm_prefix}_line l
WHERE ST_DWithin(n.geom_w_schools ,l.geom,20)
AND l.highway IS NOT NULL;
'''.format(osm_prefix = osm_prefix),
'''
-- Create table of points within 30m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
DROP TABLE IF EXISTS aos_nodes_30m_line;
CREATE TABLE aos_nodes_30m_line AS 
SELECT DISTINCT n.* 
FROM aos_nodes n, 
     {osm_prefix}_line l
WHERE ST_DWithin(n.geom_w_schools ,l.geom,30)
AND l.highway IS NOT NULL;
'''.format(osm_prefix = osm_prefix),
'''
-- Create table of points within 50m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
DROP TABLE IF EXISTS aos_nodes_50m_line;
CREATE TABLE aos_nodes_50m_line AS 
SELECT DISTINCT n.* 
FROM aos_nodes n, 
     {osm_prefix}_line l
WHERE ST_DWithin(n.geom_w_schools ,l.geom,50)
AND l.highway IS NOT NULL;
'''.format(osm_prefix = osm_prefix)
]

for sql in aos_setup:
    start = time.time()
    print("\nExecuting: {}".format(sql))
    curs.execute(sql)
    conn.commit()
    print("Executed in {} mins".format((time.time()-start)/60))
 

 
 
# pgsql to gdb
arcpy.env.workspace = db_sde_path
arcpy.env.overwriteOutput = True 
arcpy.CopyFeatures_management('public.aos_nodes_30m_line', os.path.join(gdb_path,'aos_nodes_30m_line')) 
  
# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
