-- Function for returning counts of values in an array less than a threshold distance
CREATE OR REPLACE FUNCTION count_in_threshold(distances int[],threshold int) returns bigint as $$
    SELECT COUNT(*) 
    FROM unnest(distances) dt(b)
    WHERE b < threshold
$$ language sql;

-- return minimum value of an integer array (specifically here, used for distance to closest within 3200m)
CREATE OR REPLACE FUNCTION array_min(integers int[]) returns int as $$
    SELECT min(integers) 
    FROM unnest(integers) integers
$$ language sql;

-- Create parcel level table curated for mapping purposes (ie. with Mesh Block linkage code)
DROP TABLE IF EXISTS aedc_maps_20190926_parcel;
CREATE TABLE aedc_maps_20190926_parcel AS
SELECT
    p.gnaf_pid,
    p.mb_code_2016,
    p.exclude,
    p.study_region,
    p.locale,
    a.wa_dns_1600m_dd_2018 AS dweldens,
    count_in_threshold(a.os_darray_3200m_pos_2018,800) AS n_pos_800,
    array_min(a.os_darray_3200m_pos_2018) AS dist_pos,
    count_in_threshold(a.ey_darray_3200m_cl_childcare_any_2018,1600) AS n_ecec_1600,
    count_in_threshold(a.ey_darray_3200m_cl_childcare_any_meet_nqs_2018,1600) AS n_ec_meet_1600,
    array_min(a.ey_darray_3200m_cl_childcare_any_2018) AS dist_ecec,
    array_min(a.ey_darray_3200m_cl_childcare_any_meet_nqs_2018) AS dist_ec_meet 
FROM aedc_indicators_aifs a 
LEFT JOIN parcel_indicators p USING (gnaf_pid);
CREATE UNIQUE INDEX IF NOT EXISTS ix_aedc_maps_20190926_parcel ON  aedc_maps_20190926_parcel (gnaf_pid);

-- Create Mesh Block level indicator summary table (will allow for dwelling weighting at larger area aggregations)
DROP TABLE IF EXISTS aedc_maps_20190926_mb;
CREATE TABLE aedc_maps_20190926_mb AS
SELECT a.mb_code_2016          ,
       a.mb_category_name_2016 ,
       t.study_region,
       t.locale,
       a.dwelling              ,
       a.person                ,
       a.sa1_maincode_2016     ,
       a.sa2_name_2016         ,
       a.sa3_name_2016         ,
       a.sa4_name_2016         ,
       a.gccsa_name_2016       ,
       a.state_name_2016       ,
       a.ssc_name_2016         ,
       a.lga_name_2016         ,
       a.ucl_name_2016         ,
       a.sos_name_2016         ,
       a.urban                 ,
       a.irsd_score            ,
       a.area_ha               ,
       t.dweldens              ,
       t.n_pos_800             ,
       t.dist_pos              ,
       t.n_ecec_1600           ,
       t.n_ec_meet_1600        ,
       t.dist_ecec             ,
       t.dist_ec_meet          ,
       t.sample_count          ,
       t.sample_count / a.area_ha AS sample_count_per_ha,
       a.geom                 
FROM area_linkage a 
LEFT JOIN (
    SELECT mb_code_2016,
           string_agg(DISTINCT(study_region),',')::varchar study_region,
           string_agg(DISTINCT(locale),',')::varchar locale,
           AVG(dweldens      ) AS dweldens      ,   
           AVG(n_pos_800     ) AS n_pos_800     ,      
           AVG(dist_pos      ) AS dist_pos      ,      
           AVG(n_ecec_1600   ) AS n_ecec_1600   , 
           AVG(n_ec_meet_1600) AS n_ec_meet_1600,    
           AVG(dist_ecec     ) AS dist_ecec     , 
           AVG(dist_ec_meet  ) AS dist_ec_meet  ,
           COUNT(*) AS sample_count
    FROM aedc_maps_20190926_parcel
    WHERE exclude IS NULL
    GROUP BY mb_code_2016) t USING (mb_code_2016)
WHERE a.irsd_score IS NOT NULL
  AND a.dwelling > 0
  AND a.urban = 'urban'
  AND a.study_region IS TRUE
  AND sample_count > 0
;
CREATE UNIQUE INDEX IF NOT EXISTS ix_aedc_maps_20190926_mb ON  aedc_maps_20190926_mb (mb_code_2016);
CREATE INDEX IF NOT EXISTS gix_aedc_maps_20190926_mb ON aedc_maps_20190926_mb USING GIST (geom);

-- Create SA1 dwelling weighted indicator table, for mapping
DROP TABLE IF EXISTS aedc_maps_20190926_sa1;
CREATE TABLE aedc_maps_20190926_sa1 AS
SELECT 
sa1_maincode_2016,
study_region,
locale,
SUM(dwelling) AS dwelling,
SUM(person) AS person,
SUM(sample_count) AS sample_count,
SUM(sample_count)/SUM(area_ha) AS sample_count_per_ha,
SUM(area_ha) AS area_ha,
SUM(dwelling*dweldens      ::numeric)/SUM(dwelling) AS dweldens      ,
SUM(dwelling*n_pos_800     ::numeric)/SUM(dwelling) AS n_pos_800     ,
SUM(dwelling*dist_pos      ::numeric)/SUM(dwelling) AS dist_pos      ,
SUM(dwelling*n_ecec_1600   ::numeric)/SUM(dwelling) AS n_ecec_1600   ,
SUM(dwelling*n_ec_meet_1600::numeric)/SUM(dwelling) AS n_ec_meet_1600,
SUM(dwelling*dist_ecec     ::numeric)/SUM(dwelling) AS dist_ecec     ,
SUM(dwelling*dist_ec_meet  ::numeric)/SUM(dwelling) AS dist_ec_meet  ,
ST_Union(geom) AS geom
FROM aedc_maps_20190926_mb
GROUP BY sa1_maincode_2016,
         study_region,
         locale
ORDER BY study_region,sa1_maincode_2016 ASC;
ALTER TABLE  aedc_maps_20190926_sa1 ADD PRIMARY KEY (sa1_maincode_2016);


-- Create study region dwelling weighted indicator table, for summary
DROP TABLE IF EXISTS aedc_maps_20190926_region;
CREATE TABLE aedc_maps_20190926_region AS
SELECT 
study_region,
locale,
SUM(dwelling) AS dwelling,
SUM(person) AS person,
SUM(sample_count) AS sample_count,
SUM(sample_count)/SUM(area_ha) AS sample_count_per_ha,
SUM(area_ha) AS area_ha,
SUM(dwelling*dweldens      ::numeric)/SUM(dwelling) AS dweldens      ,
SUM(dwelling*n_pos_800     ::numeric)/SUM(dwelling) AS n_pos_800     ,
SUM(dwelling*dist_pos      ::numeric)/SUM(dwelling) AS dist_pos      ,
SUM(dwelling*n_ecec_1600   ::numeric)/SUM(dwelling) AS n_ecec_1600   ,
SUM(dwelling*n_ec_meet_1600::numeric)/SUM(dwelling) AS n_ec_meet_1600,
SUM(dwelling*dist_ecec     ::numeric)/SUM(dwelling) AS dist_ecec     ,
SUM(dwelling*dist_ec_meet  ::numeric)/SUM(dwelling) AS dist_ec_meet  ,
ST_Union(geom) AS geom
FROM aedc_maps_20190926_mb
GROUP BY study_region,
         locale
ORDER BY study_region ASC;
ALTER TABLE  aedc_maps_20190926_region ADD PRIMARY KEY (study_region);

DROP TABLE IF EXISTS aedc_median_iqr;
CREATE TABLE aedc_median_iqr AS
(SELECT 
 study_region AS region,
            ROUND(percentile_disc(0.5 ) within group (order by dweldens      )::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by dweldens      )::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by dweldens      )::numeric,1) || ']' AS dweldens,
            ROUND(percentile_disc(0.5 ) within group (order by n_pos_800     )::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by n_pos_800     )::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by n_pos_800     )::numeric,1) || ']' AS n_pos_800,
            ROUND(percentile_disc(0.5 ) within group (order by dist_pos      )::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by dist_pos      )::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by dist_pos      )::numeric,1) || ']' AS dist_pos,
            ROUND(percentile_disc(0.5 ) within group (order by n_ecec_1600   )::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by n_ecec_1600   )::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by n_ecec_1600   )::numeric,1) || ']' AS n_ecec_1600,
            ROUND(percentile_disc(0.5 ) within group (order by n_ec_meet_1600)::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by n_ec_meet_1600)::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by n_ec_meet_1600)::numeric,1) || ']' AS n_ec_meet_1600,
            ROUND(percentile_disc(0.5 ) within group (order by dist_ecec     )::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by dist_ecec     )::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by dist_ecec     )::numeric,1) || ']' AS dist_ecec,
            ROUND(percentile_disc(0.5 ) within group (order by dist_ec_meet  )::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by dist_ec_meet  )::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by dist_ec_meet  )::numeric,1) || ']' AS dist_ec_meet     
 FROM aedc_maps_20190926_sa1
 GROUP BY study_region)
 UNION
(SELECT 
 '21 cities'::text AS region,
            ROUND(percentile_disc(0.5 ) within group (order by dweldens      )::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by dweldens      )::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by dweldens      )::numeric,1) || ']' AS dweldens,
            ROUND(percentile_disc(0.5 ) within group (order by n_pos_800     )::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by n_pos_800     )::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by n_pos_800     )::numeric,1) || ']' AS n_pos_800,
            ROUND(percentile_disc(0.5 ) within group (order by dist_pos      )::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by dist_pos      )::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by dist_pos      )::numeric,1) || ']' AS dist_pos,
            ROUND(percentile_disc(0.5 ) within group (order by n_ecec_1600   )::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by n_ecec_1600   )::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by n_ecec_1600   )::numeric,1) || ']' AS n_ecec_1600,
            ROUND(percentile_disc(0.5 ) within group (order by n_ec_meet_1600)::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by n_ec_meet_1600)::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by n_ec_meet_1600)::numeric,1) || ']' AS n_ec_meet_1600,
            ROUND(percentile_disc(0.5 ) within group (order by dist_ecec     )::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by dist_ecec     )::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by dist_ecec     )::numeric,1) || ']' AS dist_ecec,
            ROUND(percentile_disc(0.5 ) within group (order by dist_ec_meet  )::numeric,1) 
 || ' [' || ROUND(percentile_disc(0.25) within group (order by dist_ec_meet  )::numeric,1) 
 || ', ' || ROUND(percentile_disc(0.75) within group (order by dist_ec_meet  )::numeric,1) || ']' AS dist_ec_meet     
 FROM aedc_maps_20190926_sa1
 )
 ORDER BY region;
