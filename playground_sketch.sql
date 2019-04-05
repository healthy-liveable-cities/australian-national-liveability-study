-- First, we make sure that all the fields we want to query exist (we know most of these will
-- but for completeness we're just double checking).  
-- This means we can query fields without risk of error due to non-existance.
ALTER TABLE osm_20181001_polygon ADD COLUMN IF NOT EXISTS "access"      varchar;
ALTER TABLE osm_20181001_point   ADD COLUMN IF NOT EXISTS "access"      varchar;
ALTER TABLE osm_20181001_polygon ADD COLUMN IF NOT EXISTS "description" varchar;
ALTER TABLE osm_20181001_point   ADD COLUMN IF NOT EXISTS "description" varchar;
ALTER TABLE osm_20181001_polygon ADD COLUMN IF NOT EXISTS "landuse"     varchar;
ALTER TABLE osm_20181001_point   ADD COLUMN IF NOT EXISTS "landuse"     varchar;
ALTER TABLE osm_20181001_polygon ADD COLUMN IF NOT EXISTS "leisure"     varchar;
ALTER TABLE osm_20181001_point   ADD COLUMN IF NOT EXISTS "leisure"     varchar;
ALTER TABLE osm_20181001_polygon ADD COLUMN IF NOT EXISTS "name"        varchar;
ALTER TABLE osm_20181001_point   ADD COLUMN IF NOT EXISTS "name"        varchar;
ALTER TABLE osm_20181001_polygon ADD COLUMN IF NOT EXISTS "playground"  varchar;
ALTER TABLE osm_20181001_point   ADD COLUMN IF NOT EXISTS "playground"  varchar;

---- We make sure the point table geom
-- I've commented this out as the OSM set up script should do this
--ALTER TABLE osm_20181001_point   ADD COLUMN IF NOT EXISTS "geom"  geometry;
--UPDATE osm_20181001_point SET geom = ST_Transform(way,7845);
--ALTER TABLE osm_20181001_polygon   ADD COLUMN IF NOT EXISTS "geom"  geometry;
--UPDATE osm_20181001_point SET geom = ST_Transform(way,7845);

DROP TABLE IF EXISTS playgrounds;
CREATE TABLE playgrounds AS
WITH point AS
  (SELECT osm_20181001_point.osm_id,
           jsonb_strip_nulls(to_jsonb( 
           (SELECT d FROM 
               (SELECT 
                  "amenity",                  
                  "access",  
                  "landuse", 
                  "leisure", 
                  "natural", 
                  "sport") d)) || hstore_to_jsonb(tags) ) AS attributes,
            'point'::text AS source_type,
            geom
          FROM osm_20181001_point 
          WHERE (access IS NULL or access NOT IN ('customers','designated','employee','military','no','private','privates','staff'))
           AND (leisure = 'playground'
             OR landuse = 'playground'
             OR playground IS NOT NULL
             OR (lower(description) LIKE '%playground%' AND lower(description) NOT LIKE '%car park%')
             OR (lower(name)        LIKE '%playground%' AND lower(name)        NOT LIKE '%car park%')
             ))
SELECT row_number() OVER (ORDER BY osm_id) AS row_num,
       attributes,
       source_type,
       geom
  FROM point
 UNION (SELECT poly.osm_id,
           jsonb_strip_nulls(to_jsonb( 
           (SELECT d FROM 
               (SELECT 
                  poly."amenity",                  
                  poly."access",  
                  poly."landuse", 
                  poly."leisure", 
                  poly."natural", 
                  poly."sport") d)) || hstore_to_jsonb(poly.tags) ) AS attributes,
            'polygon'::text AS source_type,
            ST_Centroid(poly.geom) AS geom
          FROM osm_20181001_polygon poly,point 
         WHERE  (access IS NULL or access NOT IN ('customers','designated','employee','military','no','private','privates','staff'))
           AND (leisure = 'playground'
             OR landuse = 'playground'
             OR playground IS NOT NULL
             OR (lower(description) LIKE '%playground%' AND lower(description) NOT LIKE '%car park%')
             OR (lower(name)        LIKE '%playground%' AND lower(name)        NOT LIKE '%car park%')
             )
         AND NOT ST_Intersects(poly.geom,point.geom));
         
         
         
        

