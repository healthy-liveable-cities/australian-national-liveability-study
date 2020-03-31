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
a.daily_living                                     ,
a.street_connectivity                              ,
a.dwelling_density                                 ,
a.walkability_index                                ,
a.liveability_index                                ,
d.li_community_culture_leisure                     ,
d.li_early_years                                   ,
d.li_education                                     ,
d.li_health_services                               ,
d.li_sport_rec                                     ,
d.li_food                                          ,
d.li_convenience                                   ,
d.trans_07_soft  pt_reg_30_mins_400m,
d.os_public_02_soft pos_large_400m,
d.hous_01 pct_30_40_housing                                ,
d.hous_03 pct_live_work_local_area
FROM score_card_sa1_dwelling a
LEFT JOIN abs_2016_irsd b USING (sa1_maincode_2016)
LEFT JOIN sa1_2016_aust c USING (sa1_maincode_2016)
LEFT JOIN li_inds_sa1_dwelling d USING (sa1_maincode_2016)
)
TO 'D:/li_hannah_sa1_dwelling_2018_irsd_2016_20200326.csv'
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
a.daily_living                                     ,
a.street_connectivity                              ,
a.dwelling_density                                 ,
a.walkability_index                                ,
a.liveability_index                                ,
d.li_community_culture_leisure                     ,
d.li_early_years                                   ,
d.li_education                                     ,
d.li_health_services                               ,
d.li_sport_rec                                     ,
d.li_food                                          ,
d.li_convenience                                   ,
d.trans_07_soft  pt_reg_30_mins_400m,
d.os_public_02_soft pos_large_400m,
d.hous_01 pct_30_40_housing                                ,
d.hous_03 pct_live_work_local_area
FROM score_card_sa1_person a
LEFT JOIN abs_2016_irsd b USING (sa1_maincode_2016)
LEFT JOIN sa1_2016_aust c USING (sa1_maincode_2016)
LEFT JOIN li_inds_sa1_person d USING (sa1_maincode_2016)
)
TO 'D:/li_hannah_sa1_person_2018_irsd_2016_20200326.csv'
DELIMITER ',' HEADER CSV
;




