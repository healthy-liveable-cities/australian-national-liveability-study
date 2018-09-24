
-- Setting up the database
--  psql -U postgres -c   "CREATE DATABASE alburyw_2012;"
--  psql -U postgres -d alburyw_2012 -c "CREATE EXTENSION POSTGIS; CREATE EXTENSION hstore;"
--  
--  osm2pgsql -U postgres -W -l ^ 
--    -d alburyw_2012 K:/RESEARCH/GIS/Projects/ntnl_li_2018_template/data/21Cities/OSM_Roads/AlburyWodonga/AlburyWodonga_20120912.pbf ^
--    --hstore --style ./osm2pgsql/default.style
  
-- Create open space table (broad concept) 
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
--Add geometry in spatial reference for project 
ALTER TABLE open_space ADD COLUMN geom geometry; 
UPDATE open_space SET geom = ST_Transform(way,7845); 

-- Create variable for park size 
ALTER TABLE open_space ADD COLUMN area_ha double precision; 
UPDATE open_space SET area_ha = ST_Area(geom)/10000.0;

-- Create a linestring pos table 
CREATE TABLE pos_line AS 
SELECT pos_id, ST_Length(geom)::numeric AS length, geom    
FROM (SELECT pos_id, ST_ExteriorRing(geom) AS geom FROM open_space) t;

-- Generate a point every 20m along a park outlines: 
DROP TABLE pos_nodes; 
CREATE TABLE pos_nodes AS 
 WITH pos AS 
 (SELECT pos_id, 
         length, 
         generate_series(0,1,20/length) AS fraction, 
         geom FROM pos_line) 
SELECT pos_id,
       row_number() over(PARTITION BY pos_id) AS node, 
       ST_LineInterpolatePoint(geom, fraction)  AS geom 
FROM pos;

-- Create table of points within 20m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
CREATE TABLE pos_nodes_20m_line AS 
SELECT DISTINCT p.* 
FROM pos_nodes p, 
     (SELECT ST_Transform(way,7845) geom FROM planet_osm_line) l 
WHERE ST_DWithin(p.geom ,l.geom,20);


-- Create table of points within 50m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 50m of multiple roads 
CREATE TABLE pos_nodes_50m_line AS 
SELECT DISTINCT p.* 
FROM pos_nodes p, 
     (SELECT ST_Transform(way,7845) geom FROM planet_osm_line) l 
WHERE ST_DWithin(p.geom ,l.geom,50);

      
-- Create table of pos OD matrix results
CREATE TABLE pos_od 
(
participant_id INT, 
pos_id INT, 
node INT, 
distance INT, 
PRIMARY KEY (participant_id,pos_id) 
);

INSERT INTO pos_od (participant_id, pos_id, node, distance) 
SELECT DISTINCT ON (participant_id, pos_id) participant_id, pos_id, node, min(distance) 
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
   ) v(participant_id, pos_id, node, distance) 
   GROUP BY participant_id,pos_id,node 
    ON CONFLICT (participant_id,pos_id) 
       DO UPDATE 
          SET node = EXCLUDED.node, 
              distance = EXCLUDED.distance 
           WHERE pos_od.distance > EXCLUDED.distance;
      
-- Associate participants with list of parks 
DROP TABLE IF EXISTS pos_jsonb_agg; 
CREATE TABLE pos_jsonb_agg AS 
SELECT p.participant_id, 
       jsonb_agg(jsonb_strip_nulls(to_jsonb( 
           (SELECT d FROM 
               (SELECT 
                  p.distance, 
                  a.area_ha, 
                  a.access,  
                  a.landuse, 
                  a.leisure, 
                  a.natural, 
                  a.sport, 
                  a.waterway, 
                  a.wood) d)))) AS attributes 
FROM pos_od p 
LEFT JOIN open_space a ON p.pos_id = a.pos_id 
GROUP BY participant_id;   

-- Select only those items in the list which meet numeric criteria: 
SELECT participant_id, jsonb_agg(obj) AS attributes 
FROM pos_jsonb_agg, jsonb_array_elements(attributes) obj 
WHERE obj->'area_ha' > '0.5' 
GROUP BY participant_id; 
  
-- Select count for each participant matching criteria 
SELECT participant_id, jsonb_array_length(jsonb_agg(obj)) AS count 
FROM pos_jsonb_agg, jsonb_array_elements(attributes) obj 
WHERE obj->'area_ha' > '0.5'  
GROUP BY participant_id; 
   
-- Select only those items in the list which meet multiple criteria 
SELECT participant_id, jsonb_agg(obj) AS attributes 
FROM pos_jsonb_agg, jsonb_array_elements(attributes) obj 
WHERE obj->'area_ha' > '0.2'  AND obj->'distance' < '100'  
GROUP BY participant_id; 

-- Select count and return attributes for each participant matching multiple criteria
-- (mixed numeric and qualitative)
SELECT participant_id, 
       jsonb_array_length(jsonb_agg(obj)) AS count, 
       jsonb_agg(obj) AS attributes 
FROM pos_jsonb_agg, jsonb_array_elements(attributes) obj 
WHERE obj->  'leisure' = '"park"' AND obj->'distance' < '100' 
GROUP BY participant_id; 

-- return attributes in a non-json form, without grouping or filtering 
SELECT participant_id, 
       obj->'area_ha' AS area_ha,     
       obj->'distance' AS distance,    
       obj->'leisure' AS leisure 
FROM pos_jsonb_agg, jsonb_array_elements(attributes) obj; 



-- Additional test considerations for 'Areas of Open Space' (AOS)
-- This one isn't really apt; AOS area is not taken as union 
-- i think we eneed to do that explicitly
DROP TABLE IF EXISTS os_collect; 
CREATE TABLE os_collect AS 
WITH clusters AS(
    SELECT unnest(ST_ClusterWithin(open_space.geom, 20)) AS gc
       FROM open_space)
, unclustered AS( --unpacking GeomCollections
    SELECT row_number() OVER () AS cluster_id, (ST_DUMP(gc)).geom AS geom 
       FROM clusters)
SELECT cluster_id as gid, 
       jsonb_agg(jsonb_strip_nulls(to_jsonb( 
           (SELECT d FROM 
               (SELECT 
                  "pos_id",
                  "area_ha", 
                  "access",  
                  "landuse", 
                  "leisure", 
                  "natural", 
                  "sport", 
                  "waterway", 
                  "wood") d)))) AS attributes,
    COUNT(1) AS numgeom,
    ST_Collect(geom) AS geom
    FROM open_space
    INNER JOIN unclustered USING(geom)
    GROUP BY cluster_id     

-- Create variable for park size 
ALTER TABLE os_collect ADD COLUMN area_ha double precision; 
UPDATE os_collect SET area_ha = ST_Area(geom)/10000.0;   


-- Approach 2: This might be right, although
-- the issue now is with the preceding OS definition
-- OS should be broader - in particular, include schools
-- then, should subsequently create 'has school' indicator for parent AOS geom union
DROP TABLE IF EXISTS os_collect2; 
CREATE TABLE os_collect2 AS 
WITH clusters AS(
    SELECT unnest(ST_ClusterWithin(open_space.geom, 20)) AS gc
       FROM open_space)
, unclustered AS( --unpacking GeomCollections
    SELECT row_number() OVER () AS cluster_id, (ST_DUMP(gc)).geom AS geom 
       FROM clusters)
SELECT cluster_id as gid, 
       jsonb_agg(jsonb_strip_nulls(to_jsonb( 
           (SELECT d FROM 
               (SELECT 
                  "pos_id",
                  "area_ha", 
                  "access",  
                  "landuse", 
                  "leisure", 
                  "natural", 
                  "sport", 
                  "waterway", 
                  "wood") d)))) AS attributes,
    COUNT(1) AS numgeom,
    ST_Union(geom) AS geom
    FROM open_space
    INNER JOIN unclustered USING(geom)
    GROUP BY cluster_id ;    
     

-- Create variable for park size 
ALTER TABLE os_collect2 ADD COLUMN area_ha double precision; 
UPDATE os_collect2 SET area_ha = ST_Area(geom)/10000.0; 
    
--Add serial id for Areas of Open Space (AOS)
ALTER TABLE os_collect2 ADD COLUMN pos_id SERIAL PRIMARY KEY;     
    
-- Create a linestring pos table 
CREATE TABLE pos_line2 AS 
SELECT pos_id, ST_Length(geom)::numeric AS length, geom    
FROM (SELECT pos_id, ST_ExteriorRing(geom) AS geom FROM os_collect2) t;

DROP TABLE pos_nodes2; 
CREATE TABLE pos_nodes2 AS 
 WITH pos AS 
 (SELECT pos_id, 
         length, 
         generate_series(0,1,20/length) AS fraction, 
         geom FROM pos_line2) 
SELECT pos_id,
       row_number() over(PARTITION BY pos_id) AS node, 
       ST_LineInterpolatePoint(geom, fraction)  AS geom 
FROM pos;    

-- Create table of points within 20m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
CREATE TABLE pos_nodes_20m_line2 AS 
SELECT DISTINCT p.* 
FROM pos_nodes2 p, 
     (SELECT ST_Transform(way,7845) geom FROM planet_osm_line) l 
WHERE ST_DWithin(p.geom ,l.geom,20);


---  Approach 3

-- Create open space table (broad concept) 
-- Note that open space amenity 'co-locations' should be dropped if not coincident with OS,
-- and if they are coincident with OS they shouldn't count towards area of OS
-- Perhaps water shouldn't count to area either??????
-- Maybe a seperate 'water_area_ha'???
-- OR p.amenity IN ('bbq', 'bench', 'boat_rental', 'cafe','restaurant','boat_sharing', 'buggy_parking', 'drinking_water', 'firepit', 'fountain', 'library', 'marketplace', 'place_of_worship', 'public_bath', 'public_bookcase', 'restaurant', 'sauna', 'school', 'shower', 'table', 'toilets', 'waste_basket', 'waste_disposal', 'watering_place'))
DROP TABLE open_space2;
CREATE TABLE open_space2 AS 
SELECT * FROM planet_osm_polygon p 
WHERE (p.leisure IS NOT NULL 
    OR p.natural IS NOT NULL 
    OR p.sport IS NOT NULL  
    OR p.landuse IN ('forest','grass','greenfield','meadow','recreation ground','village green')
    OR p.amenity IN ('school'))
  AND (p.access IS NULL 
    OR p.access NOT IN('no','private'));

-- Create unique POS id 
ALTER TABLE open_space2 ADD COLUMN pos_id SERIAL PRIMARY KEY;         
--Add geometry in spatial reference for project 
ALTER TABLE open_space2 ADD COLUMN geom geometry; 
UPDATE open_space2 SET geom = ST_Transform(way,7845); 

ALTER TABLE open_space2 ADD COLUMN no_school_geom geometry; 
UPDATE open_space2 SET no_school_geom = ST_Transform(way,7845) WHERE amenity IS NULL;

-- Create variable for park size 
ALTER TABLE open_space2 ADD COLUMN area_ha double precision; 
UPDATE open_space2 SET area_ha = ST_Area(geom)/10000.0;


DROP TABLE IF EXISTS open_space_areas; 
CREATE TABLE open_space_areas AS 
WITH clusters AS(
    SELECT unnest(ST_ClusterWithin(open_space2.geom, .001)) AS gc
       FROM open_space2)
, unclustered AS( --unpacking GeomCollections
    SELECT row_number() OVER () AS cluster_id, (ST_DUMP(gc)).geom AS geom 
       FROM clusters)
SELECT cluster_id as gid, 
       jsonb_agg(jsonb_strip_nulls(to_jsonb( 
           (SELECT d FROM 
               (SELECT 
                  "pos_id",
                  "area_ha",
                  "amenity",                  
                  "access",  
                  "landuse", 
                  "leisure", 
                  "natural", 
                  "sport", 
                  "waterway", 
                  "wood") d)))) AS attributes,
    COUNT(1) AS numgeom,
    ST_Union(geom) AS geom
    FROM open_space2
    INNER JOIN unclustered USING(geom)
    GROUP BY cluster_id;   
 
DROP TABLE IF EXISTS open_space_areas2; 
CREATE TABLE open_space_areas2 AS 
WITH clusters AS(
    SELECT unnest(ST_ClusterWithin(open_space2.geom, .00001)) AS gc
       FROM open_space2)
, unclustered AS( --unpacking GeomCollections
    SELECT row_number() OVER () AS cluster_id, (ST_DUMP(gc)).geom AS geom 
       FROM clusters)
SELECT cluster_id as gid, 
       jsonb_agg(jsonb_strip_nulls(to_jsonb( 
           (SELECT d FROM 
               (SELECT 
                  "pos_id",
                  "area_ha",
                  "amenity",                  
                  "access",  
                  "landuse", 
                  "leisure", 
                  "natural", 
                  "sport", 
                  "waterway", 
                  "wood") d)))) AS attributes,
    COUNT(1) AS numgeom,
    ST_Union(geom) AS school_geom,
    ST_Union(no_school_geom) AS geom
    FROM open_space2
    INNER JOIN unclustered USING(geom)
    GROUP BY cluster_id;    

-- Create variable for park size 
ALTER TABLE open_space_areas2 ADD COLUMN aos_ha double precision; 
ALTER TABLE open_space_areas2 ADD COLUMN aos_ha_school double precision; 
UPDATE open_space_areas2 SET aos_ha = ST_Area(geom)/10000.0; 
UPDATE open_space_areas2 SET aos_ha_school = ST_Area(school_geom)/10000.0; 

    
-- Create a linestring pos table 
-- the 'school_bounds' prereq feature un-nests the multipolygons to straight polygons, so we can take their exterior rings
DROP TABLE pos_line4;
CREATE TABLE pos_line4 AS 
WITH school_bounds AS
   (SELECT gid, ST_SetSRID(st_astext((ST_Dump(school_geom)).geom),7845) AS school_geom  FROM open_space_areas2)
SELECT gid, ST_Length(school_geom)::numeric AS length, school_geom    
FROM (SELECT gid, ST_ExteriorRing(school_geom) AS school_geom FROM school_bounds) t;

-- Generate a point every 20m along a park outlines: 
DROP TABLE pos_nodes4; 
CREATE TABLE pos_nodes4 AS 
 WITH pos AS 
 (SELECT gid, 
         length, 
         generate_series(0,1,20/length) AS fraction, 
         school_geom FROM pos_line4) 
SELECT gid,
       row_number() over(PARTITION BY gid) AS node, 
       ST_LineInterpolatePoint(school_geom, fraction)  AS school_geom 
FROM pos;

CREATE INDEX pos_nodes4_idx ON pos_nodes4 USING GIST (school_geom);

-- Preparation of reference line feature to act like roads
ALTER TABLE planet_osm_line ADD COLUMN geom geometry; 
UPDATE planet_osm_line SET geom = ST_Transform(way,7845); 

-- Create table of points within 20m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
CREATE TABLE pos_nodes_20m_line4 AS 
SELECT DISTINCT n.* 
FROM pos_nodes4 n, 
     planet_osm_line l
WHERE ST_DWithin(n.school_geom ,l.geom,20)
AND l.highway IS NOT NULL;

-- Create table of points within 30m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
CREATE TABLE pos_nodes_30m_line4 AS 
SELECT DISTINCT n.* 
FROM pos_nodes4 n, 
     planet_osm_line l
WHERE ST_DWithin(n.school_geom ,l.geom,30)
AND l.highway IS NOT NULL;

-- Create table of points within 50m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
CREATE TABLE pos_nodes_50m_line4 AS 
SELECT DISTINCT n.* 
FROM pos_nodes4 n, 
     planet_osm_line l
WHERE ST_DWithin(n.school_geom ,l.geom,50)
AND l.highway IS NOT NULL;


--     
-- DROP TABLE IF EXISTS open_space_areas; 
-- CREATE TABLE open_space_areas AS 
-- WITH clusters AS(
--     SELECT unnest(ST_ClusterWithin(open_space.geom, 20)) AS gc
--        FROM open_space)
-- , unclustered AS( --unpacking GeomCollections
--     SELECT row_number() OVER () AS cluster_id, (ST_DUMP(gc)).geom AS geom 
--        FROM clusters)
-- SELECT cluster_id as gid, 
--        jsonb_agg(jsonb_strip_nulls(to_jsonb( 
--            (SELECT d FROM 
--                (SELECT 
--                   "pos_id",
--                   "area_ha", 
--                   "access",  
--                   "landuse", 
--                   "leisure", 
--                   "natural", 
--                   "sport", 
--                   "waterway", 
--                   "wood") d)))) AS attributes,
--     COUNT(1) AS numgeom,
--     ST_Union(geom) AS geom
--     FROM open_space
--     INNER JOIN unclustered USING(geom)
--     GROUP BY cluster_id ;    
--     
--     
-- 
-- SELECT gid, 
--               obj->'pos_id' 
-- FROM os_collect2, jsonb_array_elements(attributes) obj;
-- 
-- 
-- 
-- -----
-- 
-- DROP TABLE IF EXISTS open_space_areas2; 
-- CREATE TABLE open_space_areas2 AS 
-- WITH clusters AS(
--     (SELECT (ST_Dump(ST_Union(geom))).geom from open_space) 
--        FROM open_space)
-- SELECT row_number() OVER (geom) AS gid, 
--        jsonb_agg(jsonb_strip_nulls(to_jsonb( 
--            (SELECT d FROM 
--                (SELECT 
--                   "pos_id",
--                   "area_ha", 
--                   "access",  
--                   "landuse", 
--                   "leisure", 
--                   "natural", 
--                   "sport", 
--                   "waterway", 
--                   "wood") d)))) AS attributes,
--     COUNT(*) AS numgeom,
--     c.geom
--     FROM clusters c,open_space o
--     WHERE St_Intersects(o.geom, c.geom)
--     GROUP BY c.geom;    
--     
--     
-- WITH geoms (geom) as 
--    (SELECT (ST_Dump(ST_Union(geom))).geom from polygons) 
-- SELECT max(data), min(data), g.geom
--    FROM polygons p, geoms g 
--    WHERE St_Intersects(s.geom, g.geom)
--    GROUP BY g.geom;