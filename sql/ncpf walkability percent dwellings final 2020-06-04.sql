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
       ROUND((100 * dl_high.dwellings               / total_dwellings)::numeric,2) AS pct_high_dl_dwellings,
       ROUND((100 * sc_high.dwellings               / total_dwellings)::numeric,2) AS pct_high_sc_dwellings,
       ROUND((100 * dd_high.dwellings               / total_dwellings)::numeric,2) AS pct_high_dd_dwellings,
       ROUND((100 * walk_conditional_high.dwellings / total_dwellings)::numeric,2) AS pct_high_walk_conditional_dwellings,
       ROUND((100 * si_high.dwellings               / total_dwellings)::numeric,2) AS pct_si_walk_dwellings
FROM margins 
LEFT JOIN dl_high               USING (study_region)
LEFT JOIN sc_high               USING (study_region)
LEFT JOIN dd_high               USING (study_region)
LEFT JOIN walk_conditional_high USING (study_region)
LEFT JOIN si_high               USING (study_region)
ORDER BY study_region ASC;

COPY ncpf_walkability_percent_dwelling_analysis_2020604 TO 'D:\ntnl_li_2018_template\analysis\ncpf walkability percent dwellings final 2020-06-04.csv' CSV HEADER DELIMITER ','