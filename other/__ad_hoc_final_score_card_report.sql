DROP TABLE IF EXISTS score_card_national_summary;
CREATE TABLE score_card_national_summary AS
SELECT 
 p.study_region                                                          ,
 p.dwelling                                                              ,
 p.person                                                                ,
 ROUND(p.area_ha                   ::numeric,1) AS  area_ha                     ,
 ROUND(p.daily_living              ::numeric,1) AS  daily_living                ,
 ROUND(p.street_connectivity       ::numeric,0) AS  street_connectivity         ,
 ROUND(p.dwelling_density          ::numeric,0) AS  dwelling_density            ,
 ROUND(p.walkability_index         ::numeric,2) AS  walkability_index           ,
 ROUND(p.liveability_index         ::numeric,2) AS  liveability_index           ,
 ROUND(p.social_infrastructure_mix ::numeric,0) AS  social_infrastructure_mix   ,
 ROUND(p.closest_activity_centre   ::numeric,0) AS  closest_activity_centre     ,
 ROUND(p.closest_alcohol_offlicence::numeric,0) AS  closest_alcohol_offlicence  ,
 ROUND(p.frequent_pt_400m          ::numeric,2) AS  frequent_pt_400m            ,
 ROUND(p.large_pos_400m            ::numeric,2) AS  large_pos_400m              ,
 ROUND(p.pct_live_work_local_area  ::numeric,2) AS  pct_live_work_local_area    ,
 ROUND(d.pct_30_40_affordable_housing,2) AS  pct_30_40_affordable_housing
FROM score_card_region_person p 
LEFT JOIN score_card_region_dwelling d USING (study_region) ;

COPY score_card_national_summary 
  TO 'D:/ntnl_li_2018_template/data/study_region/_exports/score_card_national_summary.csv'
  WITH DELIMITER ','
  CSV HEADER;