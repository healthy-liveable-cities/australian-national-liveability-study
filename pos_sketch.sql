
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

--Add geometry in spatial reference for project 
ALTER TABLE open_space ADD COLUMN geom geometry; 
UPDATE open_space SET geom = ST_Transform(way,7845); 

-- Create variable for park size 
ALTER TABLE open_space ADD COLUMN area_ha double precision; 
UPDATE open_space SET area_ha = ST_Area(geom)/10000.0;

-- Create a linestring pos table 
CREATE TABLE pos_line AS 
SELECT pos_id, ST_Length(geom)::numeric AS length, geom    
FROM (SELECT id, ST_ExteriorRing(geom) AS geom FROM open_space) t;

-- Generate a point every 20m along a park outlines: 
DROP TABLE pos_nodes; 
CREATE TABLE pos_nodes AS 
 WITH pos AS 
 (SELECT pos_id, 
         length, 
         generate_series(0,1,20/length) AS fraction, 
         geom FROM pos_line) 
SELECT pos_id,
       row_number() over(PARTITION BY id) AS node, 
       ST_LineInterpolatePoint(geom, fraction)  AS geom 
FROM pos;

-- Create table of points within 20m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
CREATE TABLE pos_nodes_20m_line AS 
SELECT DISTINCT p.* 
FROM pos_nodes p, 
     (SELECT ST_Transform(way,7845) geom FROM planet_osm_line) l 
WHERE ST_DWithin(p.geom ,l.geom,20);
      
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