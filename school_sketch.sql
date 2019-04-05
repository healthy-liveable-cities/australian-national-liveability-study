-- Create table for OSM school polygons
DROP TABLE IF EXISTS school_polys;
CREATE TABLE school_polys AS 
SELECT * FROM osm_20181001_polygon p 
WHERE p.amenity IN ('school','college','university') 
   OR p.landuse IN ('school','college','university');

ALTER TABLE school_polys ADD COLUMN school_tags jsonb;       
ALTER TABLE school_polys ADD COLUMN school_count int;       
      
UPDATE school_polys o SET school_tags = NULL;
UPDATE school_polys o SET school_count = 0;
 
UPDATE school_polys t1 
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
             jsonb_agg(to_jsonb(t) - 'osm_id'::text - 'geom'::text)  AS school_tags
      FROM (SELECT DISTINCT ON (acara_scho)
            CASE 
              WHEN ST_Intersects(schools.geom, osm.geom) THEN 0
              ELSE ST_Distance(schools.geom, ST_ExteriorRing(osm.geom))::int  
            END AS distance_to_osmpoly,
            schools.*, 
            osm.osm_id
            FROM all_schools2018 schools,
                  school_polys osm
      WHERE ST_DWithin(schools.geom, ST_ExteriorRing(osm.geom), 150) OR ST_Intersects(schools.geom, osm.geom)
      ORDER BY acara_scho,ST_Distance(schools.geom, ST_ExteriorRing(osm.geom))) t
      GROUP BY osm_id) t2
 WHERE t1.osm_id = t2.osm_id;
    
DROP TABLE IF EXISTS school_matches;    
CREATE TABLE school_matches AS 
SELECT a.*, o.matched_school FROM all_schools2018 a 
                  LEFT JOIN 
                  (SELECT (jsonb_array_elements(school_tags)->>'acara_scho') AS matched_school FROM school_polys) o 
                  ON a.acara_scho::text = o.matched_school,gccsa_2018_10000m s 
                  WHERE ST_Intersects(a.geom,s.geom);

                  
-- Insert pseudo polygons for school points missing an OSM polygon match
-- Assumption is, these are genuine schools with more or less accurate geocoding
-- however they are not yet represented in OSM.  So these allow for access to them to
-- be evaluted in same way as those other existing polygons.
INSERT INTO school_polys (school_tags,school_count,geom)
SELECT jsonb_agg(to_jsonb(t) - 'geom'::text) AS school_tags,
       count(*) AS school_count,
       (ST_DUMP(ST_UNION(ST_Buffer(t.geom,10)))).geom AS geom 
 FROM
(SELECT a.* 
 FROM all_schools2018 a
 LEFT JOIN 
 (SELECT (jsonb_array_elements(school_tags)->>'acara_scho') AS matched_school FROM school_polys) o
      ON a.acara_scho::text = o.matched_school,
 gccsa_2018_10000m s 
 WHERE ST_Intersects(a.geom,s.geom)
   AND o.matched_school IS NULL) t 
GROUP BY geom;

-- if you check school matches now, there should be no points unaccounted for 
-- ie. all acara IDs are in the school_polys table
DROP TABLE IF EXISTS school_matches;    
CREATE TABLE school_matches AS 
SELECT a.*, o.matched_school FROM all_schools2018 a 
                  LEFT JOIN 
                  (SELECT (jsonb_array_elements(school_tags)->>'acara_scho') AS matched_school FROM school_polys) o 
                  ON a.acara_scho::text = o.matched_school,gccsa_2018_10000m s 
                  WHERE ST_Intersects(a.geom,s.geom);

-- -- aggregate list of attributes for schools which intersect school polygons
-- -- ie. some OSM school polygons may be associated with more than 1 school point   
-- -- QUESTION: should we remove identifying school attributes at this point?
-- UPDATE school_polys t1 
--    SET school_tags = t2.school_tags, school_count = school_count + count
--   FROM (SELECT osm_id,
--                jsonb_agg('{"dist":0}' || to_jsonb(s)) AS school_tags,
--                COUNT(*) as count
--    FROM all_schools2018 s,school_polys o 
--    WHERE ST_Intersects(s.geom, o.geom)
--    GROUP BY osm_id) t2
--    WHERE t1.osm_id = t2.osm_id;
 
-- List of schools tagged thus far
SELECT (jsonb_array_elements(school_tags)->>'acara_scho') AS schools FROM school_polys; 
 
-- OLDER NOTES
                  
-- CREATE TABLE school_test AS    
-- SELECT DISTINCT ON (acara_scho)
--         acara_scho, 
--         osm.osm_id, 
--         ST_Distance(schools.geom, ST_ExteriorRing(osm.geom))::int  as dist    
--  FROM
-- (SELECT a.* FROM all_schools2018 a LEFT JOIN school_polys o ON a.acara_scho = o.ext_school_id,gccsa_2018_10000m s WHERE ST_Intersects(a.geom,s.geom) AND osm_id IS NULL) schools,
-- school_polys osm
-- WHERE ST_DWithin(schools.geom, ST_ExteriorRing(osm.geom), 500) 
-- ORDER BY acara_scho,ST_Distance(schools.geom, ST_ExteriorRing(osm.geom));
  
DROP TABLE school_test2;
CREATE TABLE school_test2 AS    
SELECT osm_id,
       json_agg(to_jsonb(t) - 'osm_id'::text)  AS tags,
       count(*) AS school_count
FROM (
SELECT DISTINCT ON (acara_scho)
        schools.*, 
        osm.osm_id, 
        ST_Distance(schools.geom, ST_ExteriorRing(osm.geom))::int  as dist    
 FROM
(SELECT a.* FROM all_schools2018 a LEFT JOIN school_polys o ON a.acara_scho = o.ext_school_id,gccsa_2018_10000m s WHERE ST_Intersects(a.geom,s.geom) AND osm_id IS NULL) schools,
school_polys osm
WHERE ST_DWithin(schools.geom, ST_ExteriorRing(osm.geom), 100) 
ORDER BY acara_scho,ST_Distance(schools.geom, ST_ExteriorRing(osm.geom))) t
GROUP BY osm_id;
      
      
      
      
UPDATE school_polys o 
   SET ext_school_id = acara_scho::int, ext_school_dist = dist::int
  FROM (SELECT DISTINCT ON (acara_scho) 
          acara_scho, 
           s.osm_id, 
           ST_Distance(s.geom, o.geom)  as dist
        FROM all_schools2018 AS o , school_polys AS s  
        WHERE ST_DWithin(s.geom, o.geom, 500) 
        ORDER BY acara_scho, s.osm_id, ST_Distance(s.geom, o.geom)) t
 WHERE o.osm_id = t.osm_id AND o.ext_school_dist IS NULL;
 
 
 
CREATE TABLE school_check AS 
SELECT DISTINCT(a.acara_scho), 
      a.geom,
      o.osm_id,
      min(o.ext_school_dist) FROM all_schools2018 a LEFT JOIN school_polys o ON a.acara_scho = o.ext_school_id,gccsa_2018_10000m s WHERE ST_Intersects(a.geom,s.geom) GROUP BY a.acara_scho,a.geom,o.osm_id;
