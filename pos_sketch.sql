
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





--- Part 2
--- Refined version of additional operations
ALTER TABLE planet_osm_polygon ADD COLUMN geom geometry; 
UPDATE planet_osm_polygon SET geom = ST_Transform(way,7845); 


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


-- Summarise AMAL to Area Ratio
 SELECT min(amal_to_area_ratio), max(amal_to_area_ratio), avg(amal_to_area_ratio), stddev(amal_to_area_ratio) FROM open_space;
--        min        |       max        |       avg        |      stddev
-- ------------------+------------------+------------------+------------------
--  0.26033478028927 | 5066.24050198818 | 365.825404113757 | 511.459010812333


SELECT landuse,leisure,sport, COUNT(*),ROUND(avg(area_ha)::numeric,2) AS area_ha_avg,ROUND(avg(medial_axis_length)::numeric,2) AS mal,ROUND(avg(amal_to_area_ratio)::numeric,2) AS amarar_avg,ROUND(stddev(amal_to_area_ratio)::numeric,2) AS amarar_sd FROM open_space WHERE water_feature IS FALSE AND (leisure NOT IN ('track') OR leisure IS NULL) GROUP BY landuse,leisure,sport ORDER BY amarar_avg;
--  landuse |      leisure      |            sport            | count | area_ha_avg |   mal   | amarar_avg | amarar_sd
-- ---------+-------------------+-----------------------------+-------+-------------+---------+------------+-----------
--          | pitch             | softball                    |     6 |        0.13 |    0.17 |       1.35 |      0.02
--          | pitch             | lawn_bowls                  |     1 |        0.14 |    0.25 |       1.79 |
--          | nature_reserve    |                             |     6 |      624.12 | 8103.48 |      20.99 |     19.43
--          | pitch             | bowls                       |    15 |        0.14 |    2.03 |      24.39 |     71.67
--          | sports_centre     | horse_racing                |     1 |       60.34 | 1503.48 |      24.92 |
--          | pitch             | cricket;soccer              |     2 |        2.56 |   66.53 |      26.13 |      5.94
--          | pitch             | shooting                    |     1 |       17.71 |  663.20 |      37.45 |
--          | pitch             | equestrian                  |     5 |        1.72 |   58.57 |      40.21 |     17.82
--          | pitch             | rugby_league                |     3 |        0.94 |   39.75 |      43.02 |      7.71
--          | golf_course       |                             |    10 |       32.15 | 1686.71 |      46.58 |     22.22
--          | golf_course       | golf                        |     1 |       40.63 | 1942.11 |      47.80 |
--          | pitch             | soccer                      |    18 |        0.82 |   37.56 |      51.09 |     19.83
--          | pitch             | cricket;rugby_league        |     1 |        1.79 |   96.78 |      54.19 |
--          | pitch             | cricket                     |    32 |        1.24 |   84.88 |      67.47 |     36.48
--          | pitch             | field_hockey                |     6 |        0.74 |   46.24 |      68.45 |     35.47
--  forest  |                   |                             |    10 |        7.40 |  421.44 |      70.00 |     32.58
--          | pitch             | cricket;australian_football |     1 |        1.60 |  117.21 |      73.47 |
--          | pitch             | australian_football         |    18 |        1.73 |  125.20 |      73.72 |     24.56
--          | recreation_ground |                             |     9 |       16.64 |  660.13 |      88.04 |     88.81
--          | sports_centre     |                             |    35 |        6.54 |  342.61 |      92.03 |     76.09
--          | pitch             | australian_football;cricket |     2 |        1.66 |  173.61 |     105.57 |      6.41
--          | sports_centre     | bmx                         |     1 |        0.58 |   61.54 |     105.65 |
--          | pitch             | multi                       |     1 |        0.40 |   42.54 |     107.50 |
--          | sports_centre     | tennis                      |     5 |        0.97 |  151.15 |     115.79 |     94.09
--          | pitch             | baseball                    |     2 |        1.15 |  150.39 |     131.43 |     18.64
--          | sports_centre     | croquet                     |     1 |        0.26 |   37.66 |     145.30 |
--          | pitch             | beachvolleyball             |     2 |        0.05 |    6.75 |     147.73 |     29.43
--          | dog_park          |                             |     6 |        1.19 |  127.61 |     159.38 |    114.13
--          |                   | motor                       |     1 |        2.76 |  477.25 |     172.72 |
--  grass   | park              |                             |     1 |        0.36 |   64.78 |     181.08 |
--          | sports_centre     | bowling                     |     1 |        0.16 |   31.47 |     200.88 |
--          |                   |                             |   565 |       33.50 | 1026.75 |     217.38 |    215.90
--          | park              |                             |   208 |        2.31 |  321.54 |     224.37 |    134.42
--          | pitch             | basketball                  |    14 |        0.04 |   12.45 |     286.82 |     99.30
--          |                   | sailing                     |     1 |        0.04 |   12.33 |     291.58 |
--  grass   |                   |                             |    48 |        2.74 |  510.05 |     313.22 |    214.19
--          | pitch             |                             |    42 |        0.34 |   33.29 |     319.27 |    261.82
--          | pitch             | netball                     |    62 |        0.05 |   15.17 |     328.95 |     74.86
--          | pitch             | skateboard                  |     6 |        0.06 |   22.71 |     392.76 |    276.54
--  grass   | pitch             | golf                        |     5 |        0.06 |   27.00 |     476.74 |     66.48
--          | pitch             | tennis                      |   193 |        0.03 |   13.35 |     489.79 |     98.12
--          | pitch             | futsal                      |     1 |        0.03 |   14.62 |     499.74 |
--          | pitch             | skateboard;bmx              |     1 |        0.14 |   73.77 |     527.88 |
--          | playground        |                             |    30 |        0.04 |   18.61 |     632.00 |    368.61
--  grass   | miniature_golf    |                             |     1 |        0.04 |   30.97 |     820.27 |
--          | pitch             | long_jump                   |     1 |        0.05 |   90.07 |    1829.57 |
--          | pitch             | cricket_nets                |    17 |        0.01 |   17.84 |    3631.93 |   1812.02
-- (47 rows)
 
SELECT water_feature, COUNT(*),ROUND(avg(area_ha)::numeric,2) AS area_ha_avg,ROUND(avg(medial_axis_length)::numeric,2) AS mal,ROUND(avg(amal_to_area_ratio)::numeric,2) AS amarar_avg,ROUND(stddev(amal_to_area_ratio)::numeric,2) AS amarar_sd FROM open_space GROUP BY water_feature ORDER BY amarar_avg;  
--  water_feature | count | area_ha_avg |  mal   | amarar_avg | amarar_sd
-- ---------------+-------+-------------+--------+------------+-----------
--  f             |  1408 |       17.30 | 558.92 |     304.96 |    476.37
--  t             |   431 |        2.33 | 288.61 |     564.65 |    569.08
-- (2 rows)

 
SELECT leisure = 'track' AS track, COUNT(*),ROUND(avg(area_ha)::numeric,2) AS area_ha_avg,ROUND(avg(medial_axis_length)::numeric,2) AS mal,ROUND(avg(amal_to_area_ratio)::numeric,2) AS amarar_avg,ROUND(stddev(amal_to_area_ratio)::numeric,2) AS amarar_sd FROM open_space GROUP BY track ORDER BY amarar_avg;  
--  track | count | area_ha_avg |  mal   | amarar_avg | amarar_sd
-- -------+-------+-------------+--------+------------+-----------
--        |   990 |       20.34 | 740.20 |     289.48 |    269.27
--  f     |   840 |        6.20 | 203.42 |     449.28 |    683.24
--  t     |     9 |        1.63 | 853.20 |     974.44 |    557.05
-- (3 rows)



-- Create indicator for linear features informed through EDA of OS topology
ALTER TABLE open_space ADD COLUMN linear_feature boolean;
UPDATE open_space SET linear_feature = FALSE;
UPDATE open_space SET linear_feature = TRUE 
WHERE amal_to_area_ratio > 140 
  AND area_ha > 0.5 
  AND medial_axis_length > 300;

      
      
      
      

-- So, we have hints
-- We know that 
--       - water_features probably shouldn't be in area_ha
--       - nor should tracks
-- Other markers
--     - grass without leisure or sport value
--     - golf?
-- A linear feature is fine if its enclose in a non-linear feature (is covered by?)
-- So the below is okay for inclusion where contained_linear_feature is TRUE
ALTER TABLE open_space ADD COLUMN contained_linear_feature boolean;
UPDATE open_space SET contained_linear_feature = FALSE;
UPDATE open_space o SET contained_linear_feature = TRUE
FROM (SELECT pos_id,geom FROM open_space WHERE linear_feature = FALSE) nl
WHERE linear_feature IS TRUE 
 AND ST_Intersects(o.geom,nl.geom)
 AND (st_area(st_intersection(o.geom,nl.geom))/st_area(o.geom)) > .1
 OR (ST_Length(ST_CollectionExtract(ST_Intersection(o.geom,nl.geom), 2)) > 50
     AND o.pos_id < nl.pos_id 
     AND ST_Touches(o.geom,nl.geom)
     AND medial_axis_length < 500);
 
 -- That last line used to be covered by; but i think intersects is more appropriate 
 --  (who are we to say where a park ends?) and solves other problems 
 -- AND ST_CoveredBy(o.geom,nl.geom);
 -- BUT this no longer excludes neighbouring lfs .. fuck.



-- A layer of OS with no water or tracks
CREATE TABLE os_no_water_track AS
SELECT * FROM open_space WHERE water_feature IS FALSE AND (leisure NOT IN ('track') OR leisure IS NULL);


  
 
-- Create variable for school intersection 
DROP TABLE schools;
CREATE TABLE schools AS 
SELECT * FROM planet_osm_polygon p 
WHERE p.amenity IN ('school');

ALTER TABLE open_space ADD COLUMN in_school boolean; 
UPDATE open_space SET in_school = FALSE;
UPDATE open_space SET in_school = TRUE FROM schools WHERE ST_CoveredBy(open_space.geom,schools.geom);

-- Summarise parks by in_school status
SELECT in_school, COUNT(*), AVG(area_ha) FROM open_space GROUP BY in_school;







DROP TABLE IF EXISTS open_space_areas; 
CREATE TABLE open_space_areas AS 
WITH clusters AS(
    SELECT unnest(ST_ClusterWithin(open_space.geom, .001)) AS gc
       FROM open_space WHERE in_school IS FALSE AND (linear_feature IS FALSE OR contained_linear_feature IS TRUE)
    UNION
    SELECT  unnest(ST_ClusterWithin(open_space.geom, .001)) AS gc
       FROM open_space WHERE in_school IS TRUE
    UNION
    SELECT  geom AS gc
       FROM open_space WHERE linear_feature IS TRUE AND contained_linear_feature IS FALSE
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
                  "amenity",                  
                  "access",  
                  "landuse", 
                  "leisure", 
                  "natural", 
                  "sport", 
                  "waterway", 
                  "wood",
                  "medial_axis_length",
                  "amal_to_area_ratio",
                  "in_school",
                  "linear_feature",
                  "contained_linear_feature") d)))) AS attributes,
    COUNT(1) AS numgeom,
    ST_Union(geom) AS geom
    FROM open_space
    INNER JOIN unclustered USING(geom)
    GROUP BY cluster_id;   
 
 
    SELECT (CASE 
              WHEN in_school IS FALSE AND (linear_feature IS FALSE OR contained_linear_feature IS TRUE)
                THEN unnest(ST_ClusterWithin(open_space.geom, .001))
              ELSE geom
           END) AS gc
 
 
DROP TABLE IF EXISTS open_space_areas2; 
CREATE TABLE open_space_areas2 AS 
WITH clusters AS(
    SELECT unnest(ST_ClusterWithin(open_space.geom, .00001)) AS gc
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
    FROM open_space
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

