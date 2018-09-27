
-- Setting up the database
--  psql -U postgres -c   "CREATE DATABASE alburyw_2012;"
--  psql -U postgres -d alburyw_2012 -c "CREATE EXTENSION POSTGIS; CREATE EXTENSION hstore;"
--  
--  osm2pgsql -U postgres -W -l ^ 
--    -d alburyw_2012 K:/RESEARCH/GIS/Projects/ntnl_li_2018_template/data/21Cities/OSM_Roads/AlburyWodonga/AlburyWodonga_20120912.pbf ^
--    --hstore --style ./osm2pgsql/default.style
  


-- Add geom column to polygon table, appropriately transformed to project spatial reference system
ALTER TABLE planet_osm_polygon ADD COLUMN geom geometry; 
UPDATE planet_osm_polygon SET geom = ST_Transform(way,7845); 

-- Create an 'Open Space' table
DROP TABLE open_space;
CREATE TABLE open_space AS 
SELECT * FROM planet_osm_polygon p 
WHERE (p.leisure IS NOT NULL 
    OR p.natural IS NOT NULL 
    OR p.sport IS NOT NULL  
    OR p.landuse IN ('forest','grass','greenfield','meadow','recreation ground','village green'))
  AND (p.access IS NULL 
    OR p.access NOT IN('no','private'));

-- Create unique POS id 
ALTER TABLE open_space ADD COLUMN pos_id SERIAL PRIMARY KEY;         

 -- Create water feature indicator
ALTER TABLE open_space ADD COLUMN water_feature boolean;
UPDATE open_space SET water_feature = FALSE;
UPDATE open_space SET water_feature = TRUE WHERE "natural" IN ('wetland','water') OR water IS NOT NULL OR waterway IS NOT NULL OR wetland IS NOT NULL OR leisure IN ('swimming_pool','water_park') OR sport = 'swimming';

-- Create variable for park size 
ALTER TABLE open_space ADD COLUMN area_ha double precision; 
UPDATE open_space SET area_ha = ST_Area(geom)/10000.0;

-- Create variable for medial axis as a hint of linearity_index
-- https://postgis.net/2015/10/25/postgis_sfcgal_extension/
CREATE EXTENSION postgis_sfcgal; 
ALTER TABLE open_space ADD COLUMN medial_axis_length double precision; 
UPDATE open_space SET medial_axis_length = ST_Length(ST_ApproximateMedialAxis(geom));

-- Take ratio of approximate medial axis length (AMAL) to park area
ALTER TABLE open_space ADD COLUMN amal_to_area_ratio double precision; 
UPDATE open_space SET amal_to_area_ratio = medial_axis_length/area_ha;

-- get geometry of symetric difference of the convex hull of the geometry
ALTER TABLE open_space ADD COLUMN symdiff_convhull_geoms geometry; 
UPDATE open_space SET symdiff_convhull_geoms = ST_SymDifference(geom,ST_ConvexHull(geom));

-- get number of symetrically different shards from the convex hull
ALTER TABLE open_space ADD COLUMN num_symdiff_convhull_geoms double precision; 
UPDATE open_space SET num_symdiff_convhull_geoms = ST_NumGeometries(symdiff_convhull_geoms);


ALTER TABLE open_space ADD COLUMN roundness double precision; 
UPDATE open_space SET roundness = ST_Area(geom)/(ST_Area(ST_MinimumBoundingCircle(geom)));

-- Create indicator for linear features informed through EDA of OS topology
ALTER TABLE open_space ADD COLUMN linear_feature boolean;
UPDATE open_space SET linear_feature = FALSE;
UPDATE open_space SET linear_feature = TRUE 
WHERE amal_to_area_ratio > 140 
  AND area_ha > 0.5 
  AND medial_axis_length > 300
  AND num_symdiff_convhull_geoms > 0
  AND roundness < 0.25;

-- So, we have hints
-- We know that 
--       - water_features probably shouldn't be in area_ha
--       - nor should tracks
-- Other markers
--     - grass without leisure or sport value
--     - golf?
-- A linear feature is fine if its enclose in a non-linear feature (is covered by?)
-- So the below is okay for inclusion where contained_linear_feature is TRUE

-- Create 'Acceptable Linear Feature' indicator (alf?)
ALTER TABLE open_space ADD COLUMN acceptable_linear_feature boolean;
UPDATE open_space SET acceptable_linear_feature = FALSE WHERE linear_feature = TRUE;
UPDATE open_space o SET acceptable_linear_feature = TRUE
FROM (SELECT pos_id,geom FROM open_space WHERE linear_feature = FALSE) nlf
WHERE o.linear_feature IS TRUE 
 AND ST_Intersects(o.geom,nlf.geom)
 AND (st_area(st_intersection(o.geom,nlf.geom))/st_area(o.geom)) > .1
 OR (ST_Length(ST_CollectionExtract(ST_Intersection(o.geom,nlf.geom), 2)) > 50
     AND o.pos_id < nlf.pos_id 
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
  AND o.pos_id != alt.pos_id;
 
-- Create variable for school intersection 
DROP TABLE schools;
CREATE TABLE schools AS 
SELECT * FROM planet_osm_polygon p 
WHERE p.amenity IN ('school');
ALTER TABLE schools ADD COLUMN is_school boolean; 
UPDATE schools SET is_school = TRUE;

-- Set up OS for distinction based on location within a school
ALTER TABLE open_space ADD COLUMN in_school boolean; 
UPDATE open_space SET in_school = FALSE;
UPDATE open_space SET in_school = TRUE FROM schools WHERE ST_CoveredBy(open_space.geom,schools.geom);
ALTER TABLE open_space ADD COLUMN is_school boolean; 
UPDATE open_space SET is_school = FALSE;
ALTER TABLE open_space ADD COLUMN no_school_geom geometry; 
UPDATE open_space SET no_school_geom = geom WHERE is_school = FALSE;

-- Insert school polygons in open space, restricting to relevant de-identified subset of tags (ie. no school names, contact details, etc)
INSERT INTO open_space (tags,is_school,geom)
SELECT  slice(tags, 
              ARRAY['designation'     ,
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

SELECT is_school,count(*) FROM open_space GROUP BY is_school;
--  is_school | count
-- -----------+-------
--  f         |  1839
--  t         |    58


SELECT in_school,count(*) FROM open_space GROUP BY in_school;
-- in_school | count
-- ----------+-------
--           |    58
-- t         |    69
-- f         |  1770


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
SELECT cluster_id as gid, 
       jsonb_agg(jsonb_strip_nulls(to_jsonb( 
           (SELECT d FROM 
               (SELECT 
                  "pos_id",
                  "area_ha",
                  "medial_axis_length",
                  "num_symdiff_convhull_geoms",
                  "roundness",
                  "amenity",                  
                  "access",  
                  "landuse", 
                  "leisure", 
                  "natural", 
                  "sport", 
                  "waterway", 
                  "wood",
                  "in_school",
                  "is_school",
                  "water_feature",
                  "linear_feature",
                  "acceptable_linear_feature") d)) || hstore_to_jsonb(tags) )) AS attributes,
    COUNT(1) AS numgeom,
    ST_Union(no_school_geom) AS geom,
    ST_Union(geom) AS geom_w_schools
    FROM open_space
    INNER JOIN unclustered USING(geom)
    GROUP BY cluster_id;   
  
-- Create variable for park size 
ALTER TABLE open_space_areas ADD COLUMN aos_ha double precision; 
ALTER TABLE open_space_areas ADD COLUMN aos_ha_school double precision; 
-- Calculate total area of OS in Ha and where no OS is present (ie. a school without parks) set this to zero
UPDATE open_space_areas SET aos_ha = COALESCE(ST_Area(geom)/10000.0,0);
-- Calculate total area of Schools in Ha
UPDATE open_space_areas SET aos_ha_school = ST_Area(geom_w_schools)/10000.0; 
-- 
UPDATE open_space_areas SET aos_ha_school = NULL 
  WHERE gid IN (SELECT gid 
                  FROM open_space_areas,jsonb_array_elements(attributes) obj 
                  WHERE obj->'is_school' = 'false'  AND obj->'in_school' = 'false' );

-- Create variable for School OS percent
ALTER TABLE open_space_areas ADD COLUMN school_os_percent numeric; 
UPDATE open_space_areas SET school_os_percent = 100 * aos_ha/(aos_ha + aos_ha_school)::numeric; 

    
-- Select those AOS which are in fact schools, and list their contained parks
SELECT DISTINCT gid, aos_ha_school, numgeom, aos_ha, school_os_percent, jsonb_pretty(attributes) AS attributes
FROM open_space_areas, jsonb_array_elements(attributes) obj 
WHERE obj->'is_school' = 'true'
GROUP BY gid,aos_ha_school,numgeom,aos_ha,school_os_percent,attributes;
--  
-- Indicator idea: ratio of school to school OS?

    
-- Create a linestring pos table 
-- the 'school_bounds' prereq feature un-nests the multipolygons to straight polygons, so we can take their exterior rings
DROP TABLE pos_line;
CREATE TABLE pos_line AS 
WITH school_bounds AS
   (SELECT gid, ST_SetSRID(st_astext((ST_Dump(geom_w_schools)).geom),7845) AS geom_w_schools  FROM open_space_areas)
SELECT gid, ST_Length(geom_w_schools)::numeric AS length, geom_w_schools    
FROM (SELECT gid, ST_ExteriorRing(geom_w_schools) AS geom_w_schools FROM school_bounds) t;

-- Generate a point every 20m along a park outlines: 
DROP TABLE pos_nodes; 
CREATE TABLE pos_nodes AS 
 WITH pos AS 
 (SELECT gid, 
         length, 
         generate_series(0,1,20/length) AS fraction, 
         geom_w_schools FROM pos_line) 
SELECT gid,
       row_number() over(PARTITION BY gid) AS node, 
       ST_LineInterpolatePoint(geom_w_schools, fraction)  AS geom_w_schools 
FROM pos;

CREATE INDEX pos_nodes_idx ON pos_nodes USING GIST (geom_w_schools);

-- Preparation of reference line feature to act like roads
ALTER TABLE planet_osm_line ADD COLUMN geom geometry; 
UPDATE planet_osm_line SET geom = ST_Transform(way,7845); 

-- Create table of points within 20m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
DROP TABLE IF EXISTS pos_nodes_20m_line;
CREATE TABLE pos_nodes_20m_line AS 
SELECT DISTINCT n.* 
FROM pos_nodes n, 
     planet_osm_line l
WHERE ST_DWithin(n.geom_w_schools ,l.geom,20)
AND l.highway IS NOT NULL;

-- Create table of points within 30m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
DROP TABLE IF EXISTS pos_nodes_30m_line;
CREATE TABLE pos_nodes_30m_line AS 
SELECT DISTINCT n.* 
FROM pos_nodes n, 
     planet_osm_line l
WHERE ST_DWithin(n.geom_w_schools ,l.geom,30)
AND l.highway IS NOT NULL;

-- Create table of points within 50m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
DROP TABLE IF EXISTS pos_nodes_50m_line;
CREATE TABLE pos_nodes_50m_line AS 
SELECT DISTINCT n.* 
FROM pos_nodes n, 
     planet_osm_line l
WHERE ST_DWithin(n.geom_w_schools ,l.geom,50)
AND l.highway IS NOT NULL;


      
-- Create table of (hypothetical) pos OD matrix results
CREATE TABLE pos_od 
(
participant_id INT, 
aos_gid INT, 
node INT, 
distance INT, 
PRIMARY KEY (participant_id,aos_gid) 
);

INSERT INTO pos_od (participant_id, aos_gid, node, distance) 
SELECT DISTINCT ON (participant_id, aos_gid) participant_id, aos_gid, node, min(distance) 
FROM  ( 
   VALUES 
   (15151, 32, 23, 567), 
   (15151, 32, 1, 530), 
   (15151, 32, 3, 600), 
   (15151, 27, 9, 34), 
   (15152, 27, 13, 11), 
   (15152, 3, 21, 132),  
   (15152, 27, 21, 1332), 
   (15153, 27, 32, 198) 
   ) v(participant_id, aos_gid, node, distance) 
   GROUP BY participant_id,aos_gid,node 
    ON CONFLICT (participant_id,aos_gid) 
       DO UPDATE 
          SET node = EXCLUDED.node, 
              distance = EXCLUDED.distance 
           WHERE pos_od.distance > EXCLUDED.distance;
      
-- Associate participants with list of parks 
DROP TABLE IF EXISTS od_aos; 
CREATE TABLE od_aos AS 
SELECT p.participant_id, 
       jsonb_agg(jsonb_strip_nulls(to_jsonb( 
           (SELECT d FROM 
               (SELECT 
                  p.distance      ,
                  a.gid           ,
                  a.attributes    ,
                  a.numgeom       ,
                  a.aos_ha        ,
                  a.aos_ha_school ,
                  school_os_percent
                  ) d)))) AS attributes 
FROM pos_od p 
LEFT JOIN open_space_areas a ON p.aos_gid = a.gid 
GROUP BY participant_id;   

-- select set of AOS and their attributes for a particular participant
SELECT participant_id, jsonb_pretty(attributes) FROM od_aos WHERE participant_id = 15151;

-- Select only those items in the list which meet numeric criteria: 
SELECT participant_id, jsonb_agg(obj) AS attributes 
FROM open_space_areas, jsonb_array_elements(attributes) obj 
WHERE obj->'area_ha' > '0.5' 
GROUP BY participant_id; 
  
-- Select count for each participant matching criteria 
SELECT participant_id, jsonb_array_length(jsonb_agg(obj)) AS count 
FROM open_space_areas, jsonb_array_elements(attributes) obj 
WHERE obj->'area_ha' > '0.5'  
GROUP BY participant_id; 
   
-- Select only those items in the list which meet multiple criteria 
SELECT participant_id, jsonb_agg(obj) AS attributes 
FROM open_space_areas, jsonb_array_elements(attributes) obj 
WHERE obj->'area_ha' > '0.2'  AND obj->'distance' < '100'  
GROUP BY participant_id; 

-- Select count and return attributes for each participant matching multiple criteria
-- (mixed numeric and qualitative)
SELECT participant_id, 
       jsonb_array_length(jsonb_agg(obj)) AS count, 
       jsonb_agg(obj) AS attributes 
FROM open_space_areas, jsonb_array_elements(attributes) obj 
WHERE obj->  'leisure' = '"park"' AND obj->'distance' < '100' 
GROUP BY participant_id; 

-- return attributes in a non-json form, without grouping or filtering 
SELECT participant_id, 
       obj->'area_ha' AS area_ha,     
       obj->'distance' AS distance,    
       obj->'leisure' AS leisure 
FROM open_space_areas, jsonb_array_elements(attributes) obj; 



