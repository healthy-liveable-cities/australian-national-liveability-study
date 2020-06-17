-- Final revised Walkability measures for National Cities Performance Framework
-- 21 cities plus Western Sydney
-- Carl Higgs, June 2020

-- Code to be run using Australia database, containing indicator results for 21 cities 

-- First a subset of Mesh Block indicators for Western Sydney is curated
-- Then, the walkability measures for the 21 cities plus Western Sydney are summarised

-- walk_18	    Dwelling density
-- walk_19	    Street connectivity
-- walk_20_soft Daily living destination types present
-- walk_22	    Walkability index 


-- Create Western Sydney Mesh Block subset, based on LGA membership
DROP TABLE IF EXISTS li_inds_mb_dwelling_western_sydney;
CREATE TABLE li_inds_mb_dwelling_western_sydney AS
SELECT mb.* 
FROM li_inds_mb_dwelling mb
LEFT JOIN area_linkage USING (mb_code_2016)
WHERE lga_name_2016 IN ('Blue Mountains (C)','Camden (A)','Campbelltown (C) (NSW)','Fairfield (C)','Hawkesbury (C)','Liverpool (C)','Penrith (C)','Wollondilly (A)');
UPDATE li_inds_mb_dwelling_western_sydney SET study_region = 'Western Sydney';
UPDATE li_inds_mb_dwelling_western_sydney SET locale = 'western_sydney';

-- Create final walkability indicators
DROP TABLE IF EXISTS ncpf_walkability_percent_dwelling_analysis_2020604;
CREATE TABLE ncpf_walkability_percent_dwelling_analysis_2020604 AS
WITH margins AS 
    (SELECT study_region,
       COUNT(*) AS urban_meshblocks,
       sum(dwelling) AS total_dwellings
     FROM li_inds_mb_dwelling
     GROUP BY study_region
     UNION 
     SELECT study_region,
       COUNT(*) AS urban_meshblocks,
       sum(dwelling) AS total_dwellings
     FROM li_inds_mb_dwelling_western_sydney
     GROUP BY study_region)
    ,
    dl_high AS 
    (SELECT study_region,
       sum(dwelling) AS dwellings
     FROM li_inds_mb_dwelling 
     WHERE walk_20_soft >= 2.8  -- daily living score
     GROUP BY study_region
     UNION
     SELECT study_region,
       sum(dwelling) AS dwellings
     FROM li_inds_mb_dwelling_western_sydney
     WHERE walk_20_soft >= 2.8  -- daily living score
     GROUP BY study_region
     )
     ,
    sc_high AS 
    (SELECT study_region,
       sum(dwelling) AS dwellings
     FROM li_inds_mb_dwelling 
     WHERE walk_19 >= 100       -- street connectivity
     GROUP BY study_region
     UNION
     SELECT study_region,
       sum(dwelling) AS dwellings
     FROM li_inds_mb_dwelling_western_sydney
     WHERE walk_19 >= 100       -- street connectivity
     GROUP BY study_region
     )
     ,
    dd_high AS 
    (SELECT study_region,
       sum(dwelling) AS dwellings
     FROM li_inds_mb_dwelling 
     WHERE walk_18 >= 20        -- dwelling density
     GROUP BY study_region
    UNION
    SELECT study_region,
       sum(dwelling) AS dwellings
     FROM li_inds_mb_dwelling_western_sydney
     WHERE walk_18 >= 20        -- dwelling density
     GROUP BY study_region
     )
     ,
    walk_conditional_high AS  
    (SELECT study_region,
       sum(dwelling) AS dwellings
     FROM li_inds_mb_dwelling 
     WHERE walk_20_soft >= 2.8  -- daily living score
        OR
       (walk_20_soft >= 2       -- daily living score
       AND
       (
       walk_19 >= 100           -- street connectivity
       OR 
       walk_18 >= 20            -- dwelling density
       ))
     GROUP BY study_region
     UNION
     SELECT study_region,
       sum(dwelling) AS dwellings
     FROM li_inds_mb_dwelling_western_sydney
     WHERE walk_20_soft >= 2.8  -- daily living score
        OR
       (walk_20_soft >= 2       -- daily living score
       AND
       (
       walk_19 >= 100           -- street connectivity
       OR 
       walk_18 >= 20            -- dwelling density
       ))
     GROUP BY study_region
     )
    ,
    si_high AS
    (SELECT study_region,
       sum(dwelling) AS dwellings
     FROM li_inds_mb_dwelling 
     WHERE si_mix >= 8         -- social infrastructure mix
     GROUP BY study_region
     UNION
     SELECT study_region,
       sum(dwelling) AS dwellings
     FROM li_inds_mb_dwelling_western_sydney
     WHERE si_mix >= 8        -- social infrastructure mix
     GROUP BY study_region
     )
SELECT study_region,
       urban_meshblocks,
       total_dwellings,
       (100 * dl_high.dwellings               / total_dwellings)::numeric AS pct_high_dl_dwellings,
       (100 * sc_high.dwellings               / total_dwellings)::numeric AS pct_high_sc_dwellings,
       (100 * dd_high.dwellings               / total_dwellings)::numeric AS pct_high_dd_dwellings,
       (100 * walk_conditional_high.dwellings / total_dwellings)::numeric AS pct_high_walk_conditional_dwellings,
       (100 * si_high.dwellings               / total_dwellings)::numeric AS pct_si_walk_dwellings
FROM margins 
LEFT JOIN dl_high               USING (study_region)
LEFT JOIN sc_high               USING (study_region)
LEFT JOIN dd_high               USING (study_region)
LEFT JOIN walk_conditional_high USING (study_region)
LEFT JOIN si_high               USING (study_region)
ORDER BY study_region ASC;

COPY ncpf_walkability_percent_dwelling_analysis_2020604 TO 'D:\ntnl_li_2018_template\analysis\ncpf walkability percent dwellings final 2020-06-04.csv' CSV HEADER DELIMITER ',' ;



-- Create high walkability and social infrastructure Mesh Block indicators
DROP TABLE IF EXISTS ncpf_mb_walkable_neighbourhoods_summary;
CREATE TABLE ncpf_mb_walkable_neighbourhoods_summary AS
SELECT * FROM
(SELECT 
    mb_code_2016,
    study_region,
    dwelling,
    person,
    walk_18  AS "Dwelling density per hectare",
    walk_19 AS "Street connectivity per sqkm",
    walk_20_soft AS "Daily living score (/3)",
    (walk_20_soft >= 2.8 OR (walk_20_soft >= 2 AND (walk_19 >= 100 OR walk_18 >= 20 )))::int AS "Walkable neighbourhood",
    walk_22 AS "Walkability index",
    si_mix AS "Social infrastructure mix (/16)",
    geom
FROM li_inds_mb_dwelling
UNION
SELECT 
    mb_code_2016,
    study_region,
    dwelling,
    person,
    walk_18 AS "Dwelling density per hectare",
    walk_19 AS "Street connectivity per sqkm",
    walk_20_soft AS "Daily living score (/3)",
    (walk_20_soft >= 2.8 OR (walk_20_soft >= 2 AND (walk_19 >= 100 OR walk_18 >= 20 )))::int AS "Walkable neighbourhood",
    walk_22 AS "Walkability index",
    si_mix AS "Social infrastructure mix (/16)",
    geom
FROM li_inds_mb_dwelling_western_sydney) t
ORDER BY study_region, mb_code_2016;


COPY 
(SELECT study_region                               ,
 dist_m_community_centre_osm                      ,
 dist_m_hlc_2016_community_centres                ,
 dist_m_art_gallery_osm                           ,
 dist_m_cinema_osm                                ,
 dist_m_libraries                                 ,
 dist_m_museum_osm                                ,
 dist_m_theatre_osm                               ,
 dist_m_childcare_all_meet                        ,
 dist_m_childcare_oshc_meet                       ,
 "dist_m_P_12_Schools_gov"                          ,
 dist_m_primary_schools_gov                       ,
 dist_m_secondary_schools_gov                     ,
 dist_m_pharmacy_osm                              ,
 dist_m_nhsd_2017_aged_care_residential           ,
 dist_m_nhsd_2017_mc_family_health                ,
 dist_m_nhsd_2017_pharmacy                        ,
 dist_m_nhsd_2017_dentist                         ,
 dist_m_nhsd_2017_gp                              ,
 dist_m_nhsd_2017_other_community_health_care     ,
 dist_m_public_swimming_pool_osm ,
 os_public_25
 FROM li_inds_region_dwelling                          
UNION 
SELECT study_region                               ,
 dist_m_community_centre_osm                      ,
 dist_m_hlc_2016_community_centres                ,
 dist_m_art_gallery_osm                           ,
 dist_m_cinema_osm                                ,
 dist_m_libraries                                 ,
 dist_m_museum_osm                                ,
 dist_m_theatre_osm                               ,
 dist_m_childcare_all_meet                        ,
 dist_m_childcare_oshc_meet                       ,
 "dist_m_P_12_Schools_gov"                          ,
 dist_m_primary_schools_gov                       ,
 dist_m_secondary_schools_gov                     ,
 dist_m_pharmacy_osm                              ,
 dist_m_nhsd_2017_aged_care_residential           ,
 dist_m_nhsd_2017_mc_family_health                ,
 dist_m_nhsd_2017_pharmacy                        ,
 dist_m_nhsd_2017_dentist                         ,
 dist_m_nhsd_2017_gp                              ,
 dist_m_nhsd_2017_other_community_health_care     ,
 dist_m_public_swimming_pool_osm ,
 os_public_25
 FROM li_inds_region_dwelling_western_sydney
 )         
TO  
'D:\ntnl_li_2018_template\analysis\si mix distance summaries 2020-06-04.csv' CSV HEADER DELIMITER ',';

-- "Community centre "                      ,
-- "Cinema Theatre"                       ,
-- "Library"                                ,
-- "Museum Art gallery"                   ,
-- "Childcare meeting requirements all"   ,
-- "Childcare meeting requirements OSHC"  ,
-- "Public schools primary"               ,
-- "Public schools secondary"             ,
-- "Residential aged care facility"         ,
-- "Maternal child family health care"      ,
-- "Pharmacy"                               ,
-- "Dentist"                                ,
-- "General Practicitioner"                 ,
-- "Other community health care"            ,
-- "Swimming pool"                          ,
-- "Public sport recreation facility"     


DROP TABLE IF EXISTS ncpf_mb_dwellings_si_break_down;
CREATE TABLE ncpf_mb_dwellings_si_break_down AS
SELECT
s.mb_code_2016,
s.study_region,
d.total_dwelling,
(100*s.meets_si_mix_criteria/d.total_dwelling::numeric)::numeric          AS meets_si_mix_criteria,
(100*s.community_centre/d.total_dwelling::numeric)::numeric               AS  community_centre,
(100*s.cinema_theatre/d.total_dwelling::numeric)::numeric                 AS  cinema_theatre,
(100*s.library/d.total_dwelling::numeric)::numeric                        AS  library,
(100*s.museum_art_gallery/d.total_dwelling::numeric)::numeric             AS  museum_art_gallery,
(100*s.childcare_meets_all/d.total_dwelling::numeric)::numeric            AS  childcare_meets_all,
(100*s.childcare_meets_oshc/d.total_dwelling::numeric)::numeric           AS  childcare_meets_oshc,
(100*s.public_schools_primary/d.total_dwelling::numeric)::numeric         AS  public_schools_primary,
(100*s.public_schools_secondary/d.total_dwelling::numeric)::numeric       AS  public_schools_secondary,
(100*s.residential_aged_care/d.total_dwelling::numeric)::numeric          AS  residential_aged_care,
(100*s.mcf_health_care/d.total_dwelling::numeric)::numeric                AS  mcf_health_care,
(100*s.pharmacy/d.total_dwelling::numeric)::numeric                       AS  pharmacy,
(100*s.dentist/d.total_dwelling::numeric)::numeric                        AS  dentist,
(100*s.general_practicitioner/d.total_dwelling::numeric)::numeric         AS  general_practicitioner,
(100*s.other_community_health_care/d.total_dwelling::numeric)::numeric   AS  other_community_health_care,
(100*s.swimming_pool/d.total_dwelling::numeric)::numeric                 AS  swimming_pool,
(100*s.public_sport_recreation/d.total_dwelling::numeric)::numeric       AS  public_sport_recreation,
s.geom
FROM
(SELECT
mb_code_2016,
study_region,
dwelling,
person,
dwelling*((si_mix>=8)::int) AS meets_si_mix_criteria,
dwelling*COALESCE(threshold_hard(LEAST(dist_m_community_centre_osm,dist_m_hlc_2016_community_centres)::int,1000),0) AS  community_centre,
dwelling*COALESCE(threshold_hard(LEAST(dist_m_art_gallery_osm, dist_m_museum_osm)::int,3200),0)                     AS  cinema_theatre,
dwelling*COALESCE(threshold_hard(LEAST(dist_m_cinema_osm, dist_m_theatre_osm)::int,3200),0)                         AS  library,
dwelling*COALESCE(threshold_hard(dist_m_libraries::int,1000),0)                                                     AS  museum_art_gallery,
dwelling*COALESCE(threshold_hard(dist_m_childcare_all_meet::int,800),0)                                             AS  childcare_meets_all,
dwelling*COALESCE(threshold_hard(dist_m_childcare_oshc_meet::int,1600),0)                                           AS  childcare_meets_oshc,
dwelling*COALESCE(threshold_hard(LEAST(dist_m_primary_schools_gov,"dist_m_P_12_Schools_gov")::int,1600),0)          AS  public_schools_primary,
dwelling*COALESCE(threshold_hard(LEAST(dist_m_secondary_schools_gov,"dist_m_P_12_Schools_gov")::int,1600),0)        AS  public_schools_secondary,
dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_aged_care_residential::int,1000),0)                               AS  residential_aged_care,
dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_mc_family_health::int,1000),0)                                    AS  mcf_health_care,
dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_pharmacy::int,1000),0)                                            AS  pharmacy,
dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_dentist::int,1000),0)                                             AS  dentist,
dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_gp::int,1000),0)                                                  AS  general_practicitioner,
dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_other_community_health_care::int,1000),0)                         AS  other_community_health_care,
dwelling*COALESCE(threshold_hard(dist_m_public_swimming_pool_osm::int,1200),0)                                      AS  swimming_pool,
dwelling*COALESCE(threshold_hard(os_public_25::int,1000),0)                                                         AS  public_sport_recreation,
geom
FROM li_inds_mb_dwelling
UNION
SELECT
mb.mb_code_2016,
'Western Sydney' AS study_region,
mb.dwelling,
mb.person,
mb.dwelling*((si_mix>=8)::int) AS meets_si_mix_criteria,
mb.dwelling*COALESCE(threshold_hard(LEAST(dist_m_community_centre_osm,dist_m_hlc_2016_community_centres)::int,1000),0) AS  community_centre,
mb.dwelling*COALESCE(threshold_hard(LEAST(dist_m_art_gallery_osm, dist_m_museum_osm)::int,3200),0)                     AS  cinema_theatre,
mb.dwelling*COALESCE(threshold_hard(LEAST(dist_m_cinema_osm, dist_m_theatre_osm)::int,3200),0)                         AS  library,
mb.dwelling*COALESCE(threshold_hard(dist_m_libraries::int,1000),0)                                                     AS  museum_art_gallery,
mb.dwelling*COALESCE(threshold_hard(dist_m_childcare_all_meet::int,800),0)                                             AS  childcare_meets_all,
mb.dwelling*COALESCE(threshold_hard(dist_m_childcare_oshc_meet::int,1600),0)                                           AS  childcare_meets_oshc,
mb.dwelling*COALESCE(threshold_hard(LEAST(dist_m_primary_schools_gov,"dist_m_P_12_Schools_gov")::int,1600),0)          AS  public_schools_primary,
mb.dwelling*COALESCE(threshold_hard(LEAST(dist_m_secondary_schools_gov,"dist_m_P_12_Schools_gov")::int,1600),0)        AS  public_schools_secondary,
mb.dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_aged_care_residential::int,1000),0)                               AS  residential_aged_care,
mb.dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_mc_family_health::int,1000),0)                                    AS  mcf_health_care,
mb.dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_pharmacy::int,1000),0)                                            AS  pharmacy,
mb.dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_dentist::int,1000),0)                                             AS  dentist,
mb.dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_gp::int,1000),0)                                                  AS  general_practicitioner,
mb.dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_other_community_health_care::int,1000),0)                         AS  other_community_health_care,
mb.dwelling*COALESCE(threshold_hard(dist_m_public_swimming_pool_osm::int,1200),0)                                      AS  swimming_pool,
mb.dwelling*COALESCE(threshold_hard(os_public_25::int,1000),0)                                                         AS  public_sport_recreation,
mb.geom
FROM li_inds_mb_dwelling mb
LEFT JOIN area_linkage USING (mb_code_2016)
WHERE lga_name_2016 IN ('Blue Mountains (C)','Camden (A)','Campbelltown (C) (NSW)','Fairfield (C)','Hawkesbury (C)','Liverpool (C)','Penrith (C)','Wollondilly (A)')) s
LEFT JOIN
(SELECT
study_region,
SUM(dwelling) AS total_dwelling
FROM li_inds_mb_dwelling
GROUP BY study_region
UNION
SELECT
'Western Sydney' AS study_region,
SUM(mb.dwelling) AS total_dwelling
FROM li_inds_mb_dwelling mb
LEFT JOIN area_linkage USING (mb_code_2016)
WHERE lga_name_2016 IN ('Blue Mountains (C)','Camden (A)','Campbelltown (C) (NSW)','Fairfield (C)','Hawkesbury (C)','Liverpool (C)','Penrith (C)','Wollondilly (A)')) d
USING (study_region);



DROP TABLE IF EXISTS ncpf_region_dwellings_si_break_down;
CREATE TABLE ncpf_region_dwellings_si_break_down AS
SELECT
s.study_region,
d.total_dwelling,
(100*SUM(s.meets_si_mix_criteria)/d.total_dwelling::numeric)::numeric          AS meets_si_mix_criteria,
(100*SUM(s.community_centre)/d.total_dwelling::numeric)::numeric               AS  community_centre,
(100*SUM(s.cinema_theatre)/d.total_dwelling::numeric)::numeric                 AS  cinema_theatre,
(100*SUM(s.library)/d.total_dwelling::numeric)::numeric                        AS  library,
(100*SUM(s.museum_art_gallery)/d.total_dwelling::numeric)::numeric             AS  museum_art_gallery,
(100*SUM(s.childcare_meets_all)/d.total_dwelling::numeric)::numeric            AS  childcare_meets_all,
(100*SUM(s.childcare_meets_oshc)/d.total_dwelling::numeric)::numeric           AS  childcare_meets_oshc,
(100*SUM(s.public_schools_primary)/d.total_dwelling::numeric)::numeric         AS  public_schools_primary,
(100*SUM(s.public_schools_secondary)/d.total_dwelling::numeric)::numeric       AS  public_schools_secondary,
(100*SUM(s.residential_aged_care)/d.total_dwelling::numeric)::numeric          AS  residential_aged_care,
(100*SUM(s.mcf_health_care)/d.total_dwelling::numeric)::numeric                AS  mcf_health_care,
(100*SUM(s.pharmacy)/d.total_dwelling::numeric)::numeric                       AS  pharmacy,
(100*SUM(s.dentist)/d.total_dwelling::numeric)::numeric                        AS  dentist,
(100*SUM(s.general_practicitioner)/d.total_dwelling::numeric)::numeric         AS  general_practicitioner,
(100*SUM(s.other_community_health_care)/d.total_dwelling::numeric)::numeric   AS  other_community_health_care,
(100*SUM(s.swimming_pool)/d.total_dwelling::numeric)::numeric                 AS  swimming_pool,
(100*SUM(s.public_sport_recreation)/d.total_dwelling::numeric)::numeric       AS  public_sport_recreation
FROM
(SELECT
mb_code_2016,
study_region,
dwelling,
person,
dwelling*((si_mix>=8)::int) AS meets_si_mix_criteria,
dwelling*COALESCE(threshold_hard(LEAST(dist_m_community_centre_osm,dist_m_hlc_2016_community_centres)::int,1000),0) AS  community_centre,
dwelling*COALESCE(threshold_hard(LEAST(dist_m_art_gallery_osm, dist_m_museum_osm)::int,3200),0)                     AS  cinema_theatre,
dwelling*COALESCE(threshold_hard(LEAST(dist_m_cinema_osm, dist_m_theatre_osm)::int,3200),0)                         AS  library,
dwelling*COALESCE(threshold_hard(dist_m_libraries::int,1000),0)                                                     AS  museum_art_gallery,
dwelling*COALESCE(threshold_hard(dist_m_childcare_all_meet::int,800),0)                                             AS  childcare_meets_all,
dwelling*COALESCE(threshold_hard(dist_m_childcare_oshc_meet::int,1600),0)                                           AS  childcare_meets_oshc,
dwelling*COALESCE(threshold_hard(LEAST(dist_m_primary_schools_gov,"dist_m_P_12_Schools_gov")::int,1600),0)          AS  public_schools_primary,
dwelling*COALESCE(threshold_hard(LEAST(dist_m_secondary_schools_gov,"dist_m_P_12_Schools_gov")::int,1600),0)        AS  public_schools_secondary,
dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_aged_care_residential::int,1000),0)                               AS  residential_aged_care,
dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_mc_family_health::int,1000),0)                                    AS  mcf_health_care,
dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_pharmacy::int,1000),0)                                            AS  pharmacy,
dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_dentist::int,1000),0)                                             AS  dentist,
dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_gp::int,1000),0)                                                  AS  general_practicitioner,
dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_other_community_health_care::int,1000),0)                         AS  other_community_health_care,
dwelling*COALESCE(threshold_hard(dist_m_public_swimming_pool_osm::int,1200),0)                                      AS  swimming_pool,
dwelling*COALESCE(threshold_hard(os_public_25::int,1000),0)                                                         AS  public_sport_recreation,
geom
FROM li_inds_mb_dwelling
UNION
SELECT
mb.mb_code_2016,
'Western Sydney' AS study_region,
mb.dwelling,
mb.person,
mb.dwelling*((si_mix>=8)::int) AS meets_si_mix_criteria,
mb.dwelling*COALESCE(threshold_hard(LEAST(dist_m_community_centre_osm,dist_m_hlc_2016_community_centres)::int,1000),0) AS  community_centre,
mb.dwelling*COALESCE(threshold_hard(LEAST(dist_m_art_gallery_osm, dist_m_museum_osm)::int,3200),0)                     AS  cinema_theatre,
mb.dwelling*COALESCE(threshold_hard(LEAST(dist_m_cinema_osm, dist_m_theatre_osm)::int,3200),0)                         AS  library,
mb.dwelling*COALESCE(threshold_hard(dist_m_libraries::int,1000),0)                                                     AS  museum_art_gallery,
mb.dwelling*COALESCE(threshold_hard(dist_m_childcare_all_meet::int,800),0)                                             AS  childcare_meets_all,
mb.dwelling*COALESCE(threshold_hard(dist_m_childcare_oshc_meet::int,1600),0)                                           AS  childcare_meets_oshc,
mb.dwelling*COALESCE(threshold_hard(LEAST(dist_m_primary_schools_gov,"dist_m_P_12_Schools_gov")::int,1600),0)          AS  public_schools_primary,
mb.dwelling*COALESCE(threshold_hard(LEAST(dist_m_secondary_schools_gov,"dist_m_P_12_Schools_gov")::int,1600),0)        AS  public_schools_secondary,
mb.dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_aged_care_residential::int,1000),0)                               AS  residential_aged_care,
mb.dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_mc_family_health::int,1000),0)                                    AS  mcf_health_care,
mb.dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_pharmacy::int,1000),0)                                            AS  pharmacy,
mb.dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_dentist::int,1000),0)                                             AS  dentist,
mb.dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_gp::int,1000),0)                                                  AS  general_practicitioner,
mb.dwelling*COALESCE(threshold_hard(dist_m_nhsd_2017_other_community_health_care::int,1000),0)                         AS  other_community_health_care,
mb.dwelling*COALESCE(threshold_hard(dist_m_public_swimming_pool_osm::int,1200),0)                                      AS  swimming_pool,
mb.dwelling*COALESCE(threshold_hard(os_public_25::int,1000),0)                                                         AS  public_sport_recreation,
mb.geom
FROM li_inds_mb_dwelling mb
LEFT JOIN area_linkage USING (mb_code_2016)
WHERE lga_name_2016 IN ('Blue Mountains (C)','Camden (A)','Campbelltown (C) (NSW)','Fairfield (C)','Hawkesbury (C)','Liverpool (C)','Penrith (C)','Wollondilly (A)')) s
LEFT JOIN
(SELECT
study_region,
SUM(dwelling) AS total_dwelling
FROM li_inds_mb_dwelling
GROUP BY study_region
UNION
SELECT
'Western Sydney' AS study_region,
SUM(mb.dwelling) AS total_dwelling
FROM li_inds_mb_dwelling mb
LEFT JOIN area_linkage USING (mb_code_2016)
WHERE lga_name_2016 IN ('Blue Mountains (C)','Camden (A)','Campbelltown (C) (NSW)','Fairfield (C)','Hawkesbury (C)','Liverpool (C)','Penrith (C)','Wollondilly (A)')) d
USING (study_region)
GROUP BY study_region, total_dwelling
ORDER BY meets_si_mix_criteria DESC;

COPY ncpf_region_dwellings_si_break_down TO  'D:\ntnl_li_2018_template\analysis\ncpf_pct_meet_si_mix_breakdown_2020-06-04.csv' CSV HEADER DELIMITER ',';
