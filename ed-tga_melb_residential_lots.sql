DROP TABLE IF EXISTS melb_ed_tga_residential_lots;
CREATE TABLE melb_ed_tga_residential_lots AS
SELECT
    p.gnaf_pid                        ,        
    abs.mb_code_2016                  ,
    abs.mb_category_name_2016         ,
    abs.dwelling                      ,
    abs.person                        ,
    abs.sa1_maincode                  ,
    abs.sa2_name_2016                 ,
    abs.sa3_name_2016                 ,
    abs.sa4_name_2016                 ,
    abs.gccsa_name                    ,
    abs.state_name                    ,
    dd.dwellings AS dwellings_nh1600m ,                              
    dd.dd_nh1600m                     ,
    sc.intersection_count             ,         
    sc.sc_nh1600m                     ,         
    dl.dl_hard_1600m                  ,        
    dl.dl_soft_1600m                  ,          
    wa.wa_hard_1600m                  ,            
    wa.wa_soft_1600m                  ,            
    os.pos_any_distance_m             AS pos_any_distance_3200m             ,
    os.pos_5k_sqm_distance_m          AS pos_5k_sqm_distance_3200m          ,
    os.pos_15k_sqm_distance_m         AS pos_15k_sqm_distance_3200m         ,
    os.pos_20k_sqm_distance_m         AS pos_20k_sqm_distance_3200m         ,
    os.pos_4k_10k_sqm_distance_m      AS pos_4k_10k_sqm_distance_3200m      ,
    os.pos_10k_50k_sqm_distance_m     AS pos_10k_50k_sqm_distance_3200m     ,
    os.pos_50k_200k_sqm_distance_m    AS pos_50k_200k_sqm_distance_3200m    ,
    os.pos_50k_sqm_distance_m         AS pos_50k_sqm_distance_3200m         ,
    os.sport_distance_m               AS sport_distance_3200m               ,
    os.pos_toilet_distance_m          AS pos_toilet_distance_3200m          ,
    nh.pt_any_gtfs_hlc_2018           ,
    nh.pt_freq_gtfs_hlc_2018          ,
    d.gtfs_2018_stops_bus             ,
    d.gtfs_2018_stops_train           ,
    d.gtfs_2018_stops_tram            ,
    nh.activity_centres_hlc_2017      ,
    nh.convenience_osm_2018           ,
    nh.supermarket_hlc_2017           ,
    nh.supermarket_osm_2018           ,
    nh.childcare_meets_acequa_2019    ,
    nh.primary_school_acara_2017      ,
    nh.secondary_school_acara_2017    ,
    nh.community_pow_osm_2018         ,
    nh.libraries_hlc_2018             ,
    nh.postoffice_osm_2018            ,
    nh.dentist_nhsd_2017              ,
    nh.pharmacy_nhsd_2017             ,
    nh.gp_nhsd_2017                   ,
    nh.food_fresh_specialty_osm_2018  ,
    nh.food_fast_hlc_2017             ,
    nh.food_fast_osm_2018             ,
    nh.food_dining_osm_2018           ,
    nh.culture_osm_2018               ,
    nh.alcohol_nightlife_osm_2018     ,
    nh.alcohol_offlicence_osm_2018    ,
    nh.alcohol_offlicence_hlc_2017_19 ,
    nh.alcohol_onlicence_hlc_2017_19  ,
    nh.tobacco_osm_2018               ,
    nh.gambling_osm_2018              ,
    fd.food_count_supermarkets_3200m     ,
    fd.food_count_fruit_veg_3200m        ,
    fd.food_count_other_specialty_3200m  ,
    fd.food_count_healthier_3200m  ,
    fd.food_count_fastfood_3200m       ,
    fd.food_healthy_proportion_3200m,
    fd.food_healthy_ratio_3200m,
    fd.food_fresh_proportion_3200m,
    fd.food_fresh_ratio_3200m,
    ST_AsText(ST_Transform(p.geom,4326)) AS wkt_epsg4326
FROM parcel_dwellings p
LEFT JOIN abs_linkage      abs ON p.mb_code_20 = abs.mb_code_2016          
LEFT JOIN dd_nh1600m       dd  ON p.gnaf_pid   = dd.gnaf_pid
LEFT JOIN sc_nh1600m       sc  ON p.gnaf_pid   = sc.gnaf_pid
LEFT JOIN ind_daily_living dl  ON p.gnaf_pid   = dl.gnaf_pid
LEFT JOIN ind_walkability  wa  ON p.gnaf_pid   = wa.gnaf_pid
LEFT JOIN ind_os_distance  os  ON p.gnaf_pid   = os.gnaf_pid
LEFT JOIN nh_inds_distance nh  ON p.gnaf_pid   = nh.gnaf_pid
LEFT JOIN dest_distance_m   d  ON p.gnaf_pid   =  d.gnaf_pid
LEFT JOIN ind_food         fd  ON p.gnaf_pid   = fd.gnaf_pid
;

COPY melb_ed_tga_residential_lots TO 'D:/ntnl_li_2018_template/data/ed-tga_melbourne_2018_20190403.csv' WITH DELIMITER ',' CSV HEADER;  