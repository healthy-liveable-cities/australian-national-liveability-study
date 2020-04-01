COPY
(SELECT 
a.sa1_maincode_2016                                ,
c.sa2_name_2016                                    , 
c.sa3_name_2016                                    , 
c.sa4_name_2016                                    , 
a.study_region                                     ,
a.locale                                           ,
b.usual_resident_pop     full_usual_resident_pop   ,
b.irsd_score             irsd_score_2016           ,
b.aust_rank              irsd_aust_rank            ,
b.aust_decile            irsd_aust_decile          ,
b.aust_pctile            irsd_aust_pctile          ,
b.state                  irsd_state                ,
b.state_rank             irsd_state_rank           ,
b.state_decile           irsd_state_decile         ,
b.state_pctile           irsd_state_pctile         ,
a.dwelling               urban_dwelling            ,
a.person                 urban_person              ,
a.area_ha                urban_area_ha             ,
a.walk_18                dwelling_density          ,
a.walk_19                street_connectivity       ,
a.walk_20_soft           daily_living              ,
a.walk_21_soft           local_living              ,
a.walk_22                walkability_index         ,
a.uli                    liveability_index         ,
a.li_community_culture_leisure                     ,
a.li_early_years                                   ,
a.li_education                                     ,
a.li_health_services                               ,
a.li_sport_rec                                     ,
a.li_food                                          ,
a.li_convenience                                   ,
a.trans_07_soft  pt_reg_30_mins_400m               ,
a.os_public_02_soft pos_large_400m                 ,
a.hous_01 pct_30_40_housing                        ,
a.hous_03 pct_live_work_local_area
FROM li_inds_sa1_dwelling a
LEFT JOIN abs_2016_irsd b USING (sa1_maincode_2016)
LEFT JOIN sa1_2016_aust c USING (sa1_maincode_2016)
)
TO 'D:/li_hannah_sa1_dwelling_2018_irsd_2016_20200331.csv'
DELIMITER ',' HEADER CSV
;

COPY
(SELECT 
a.sa1_maincode_2016                                ,
c.sa2_name_2016                                    , 
c.sa3_name_2016                                    , 
c.sa4_name_2016                                    , 
a.study_region                                     ,
a.locale                                           ,
b.usual_resident_pop     full_usual_resident_pop   ,
b.irsd_score             irsd_score_2016           ,
b.aust_rank              irsd_aust_rank            ,
b.aust_decile            irsd_aust_decile          ,
b.aust_pctile            irsd_aust_pctile          ,
b.state                  irsd_state                ,
b.state_rank             irsd_state_rank           ,
b.state_decile           irsd_state_decile         ,
b.state_pctile           irsd_state_pctile         ,
a.dwelling               urban_dwelling            ,
a.person                 urban_person              ,
a.area_ha                urban_area_ha             ,
a.walk_18                dwelling_density          ,
a.walk_19                street_connectivity       ,
a.walk_20_soft           daily_living              ,
a.walk_21_soft           local_living              ,
a.walk_22                walkability_index         ,
a.uli                    liveability_index         ,
a.li_community_culture_leisure                     ,
a.li_early_years                                   ,
a.li_education                                     ,
a.li_health_services                               ,
a.li_sport_rec                                     ,
a.li_food                                          ,
a.li_convenience                                   ,
a.trans_07_soft  pt_reg_30_mins_400m               ,
a.os_public_02_soft pos_large_400m                 ,
a.hous_01 pct_30_40_housing                        ,
a.hous_03 pct_live_work_local_area
FROM  li_inds_sa1_person a
LEFT JOIN abs_2016_irsd b USING (sa1_maincode_2016)
LEFT JOIN sa1_2016_aust c USING (sa1_maincode_2016)
)
TO 'D:/li_hannah_sa1_person_2018_irsd_2016_20200331.csv'
DELIMITER ',' HEADER CSV
;




