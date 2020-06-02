 
CREATE TABLE aos_acara_naplan AS 
SELECT aos_id,  
       acara.acara_school_id, 
       -- get the seperate potential naplan scores (not all are recorded)
       year3_reading , 
       year3_writing , 
       year3_spelling, 
       year3_grammar , 
       year3_numeracy, 
       year5_reading , 
       year5_writing , 
       year5_spelling, 
       year5_grammar , 
       year5_numeracy, 
       year7_reading , 
       year7_writing , 
       year7_spelling, 
       year7_grammar , 
       year7_numeracy, 
       year9_reading , 
       year9_writing , 
       year9_spelling, 
       year9_grammar , 
       year9_numeracy, 
       -- take the sum of naplan scores for a school
       COALESCE(year3_reading ,0)+ 
       COALESCE(year3_writing ,0)+ 
       COALESCE(year3_spelling,0)+ 
       COALESCE(year3_grammar ,0)+ 
       COALESCE(year3_numeracy,0)+ 
       COALESCE(year5_reading ,0)+ 
       COALESCE(year5_writing ,0)+ 
       COALESCE(year5_spelling,0)+ 
       COALESCE(year5_grammar ,0)+ 
       COALESCE(year5_numeracy,0)+ 
       COALESCE(year7_reading ,0)+ 
       COALESCE(year7_writing ,0)+ 
       COALESCE(year7_spelling,0)+ 
       COALESCE(year7_grammar ,0)+ 
       COALESCE(year7_numeracy,0)+ 
       COALESCE(year9_reading ,0)+ 
       COALESCE(year9_writing ,0)+ 
       COALESCE(year9_spelling,0)+ 
       COALESCE(year9_grammar ,0)+ 
       COALESCE(year9_numeracy,0) AS sum, 
       -- take the non-null count of naplan scores for a school
       (select count(*) 
        from (values  
                    (year3_reading ), 
                    (year3_writing ), 
                    (year3_spelling), 
                    (year3_grammar ), 
                    (year3_numeracy), 
                    (year5_reading ), 
                    (year5_writing ), 
                    (year5_spelling), 
                    (year5_grammar ), 
                    (year5_numeracy), 
                    (year7_reading ), 
                    (year7_writing ), 
                    (year7_spelling), 
                    (year7_grammar ), 
                    (year7_numeracy), 
                    (year9_reading ), 
                    (year9_writing ), 
                    (year9_spelling), 
                    (year9_grammar ), 
                    (year9_numeracy) 
        ) as v(col) 
        where v.col is not null 
       ) as non_null_count 
FROM     
   -- extract school ids from open space table 
   (SELECT aos_id,  
         (tags.value->>'acara_scho')::int AS acara_school_id  
    FROM open_space_areas schools, 
         jsonb_array_elements(schools.attributes) obj, 
         jsonb_array_elements((obj ->>'school_tags')::jsonb) tags) acara 
    -- join schools with their naplan scores 
LEFT JOIN acara_schools ON acara.acara_school_id = acara_schools.acara_school_id 
WHERE acara.acara_school_id IS NOT NULL; 
-- create index 
CREATE UNIQUE INDEX aos_acara_naplan_idx ON  aos_acara_naplan (aos_id,acara_school_id);  
 
\d aos_acara_naplan 
 
-- get average scores for all schools within 3200m of participants (rows indexed by address and school) 
SELECT p.gnaf_pid,  
       naplan.acara_school_id, 
       o.distance, 
       ---- Commented out the individual year results (n=20)  
       ---- as then the results will have too many columns to fit on one page! 
       -- year3_reading , 
       -- year3_writing , 
       -- year3_spelling, 
       -- year3_grammar , 
       -- year3_numeracy, 
       -- year5_reading , 
       -- year5_writing , 
       -- year5_spelling, 
       -- year5_grammar , 
       -- year5_numeracy, 
       -- year7_reading , 
       -- year7_writing , 
       -- year7_spelling, 
       -- year7_grammar , 
       -- year7_numeracy, 
       -- year9_reading , 
       -- year9_writing , 
       -- year9_spelling, 
       -- year9_grammar , 
       -- year9_numeracy, 
       sum AS naplan_sum, 
       non_null_count AS naplan_test_count, 
       sum/ nullif(non_null_count::float,0) AS naplan_average 
  FROM parcel_dwellings p 
  LEFT JOIN  
     -- get the distances and ids for all parks within 3.2km 
     (SELECT gnaf_pid, 
            (obj->>'aos_id')::int AS aos_id, 
            (obj->>'distance')::int AS distance 
      FROM od_aos_jsonb, 
           jsonb_array_elements(attributes) obj) o  
  ON p.gnaf_pid = o.gnaf_pid 
  LEFT JOIN aos_acara_naplan naplan 
  ON o.aos_id = naplan.aos_id 
  WHERE naplan.acara_school_id IS NOT NULL 
  LIMIT 100; 
 
-- Get average of all schools within 800m of address (rows indexed by address) 
SELECT p.gnaf_pid,  
       COUNT(acara_school_id) AS school_count, 
       AVG(o.distance)::int AS average_distance_m, 
       AVG(sum) AS average_sum_of_naplan_3200m, 
       AVG(non_null_count) AS average_test_count_3200m, 
       AVG(sum/ nullif(non_null_count::float,0)) AS naplan_average_3200m 
  FROM parcel_dwellings p 
  LEFT JOIN  
     -- get the distances and ids for all parks within 3.2km 
     (SELECT gnaf_pid, 
            (obj->>'aos_id')::int AS aos_id, 
            (obj->>'distance')::int AS distance 
      FROM od_aos_jsonb, 
           jsonb_array_elements(attributes) obj) o  
  ON p.gnaf_pid = o.gnaf_pid 
  LEFT JOIN aos_acara_naplan naplan ON o.aos_id = naplan.aos_id 
  WHERE naplan.acara_school_id IS NOT NULL 
    AND o.distance < 800
  GROUP BY p.gnaf_pid 
  LIMIT 100; 
 
   
   
