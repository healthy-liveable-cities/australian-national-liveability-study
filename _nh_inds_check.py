# Script:  16_neighbourhood_indicators.py
# Purpose: Compile destinations results and neighbourhood indicator tables
# Author:  Carl Higgs 
# Date:    20190412

import os
import sys
import time
import psycopg2 

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

# Neighbourhood_indicators
print("Create nh_inds_distance (curated distance to closest table for re-use by other indicators)... "),
nh_distance = '''
DROP TABLE IF EXISTS {table};
CREATE TABLE IF NOT EXISTS {table} AS
SELECT 
       {id},
       activity_centres_2017                                                       AS activity_centres_hlc_2017     , 
       LEAST(convenience_osm,newsagent_osm,petrolstation_osm,market_osm)           AS convenience_osm_2018          , 
       supermarkets_2017                                                           AS supermarket_hlc_2017          , 
       supermarket_osm                                                             AS supermarket_osm_2018          , 
       gtfs_2018_stops                                                             AS pt_any_gtfs_hlc_2018          ,
       gtfs_2018_stop_30_mins_final                                                AS pt_freq_gtfs_hlc_2018         ,
       childcare_all_meet_2019                                                     AS childcare_meets_acequa_2019   ,
       primary_schools2018                                                         AS primary_school_acara_2017     ,
       secondary_schools2018                                                       AS secondary_school_acara_2017   ,
       LEAST(community_centre_osm,place_of_worship_osm)                            AS community_pow_osm_2018        ,
       libraries_2018                                                              AS libraries_hlc_2018            ,
       postoffice_osm                                                              AS postoffice_osm_2018           ,
       nhsd_2017_dentist                                                           AS dentist_nhsd_2017             ,
       nhsd_2017_pharmacy                                                          AS pharmacy_nhsd_2017            ,
       nhsd_2017_gp                                                                AS gp_nhsd_2017                  ,
       LEAST(bakery_osm,meat_seafood_osm,fruit_veg_osm,deli_osm)                   AS food_fresh_specialty_osm_2018 ,
       fastfood_2017                                                               AS food_fast_hlc_2017            ,
       LEAST(fastfood_osm,food_court_osm)                                          AS food_fast_osm_2018            ,
       LEAST(restaurant_osm,cafe_osm,pub_osm)                                      AS food_dining_osm_2018          ,
       LEAST(museum_osm, theatre_osm, cinema_osm, art_gallery_osm, art_centre_osm) AS culture_osm_2018              ,
       LEAST(bar_osm, nightclub_osm,pub_osm)                                       AS alcohol_nightlife_osm_2018    ,
       alcohol_osm                                                                 AS alcohol_offlicence_osm_2018   ,
       alcohol_offlicence                                                          AS alcohol_offlicence_hlc_2017_19,
       alcohol_onlicence                                                           AS alcohol_onlicence_hlc_2017_19 ,  
       tobacco_osm                                                                 AS tobacco_osm_2018              ,
       gambling_osm                                                                AS gambling_osm_2018           
FROM dest_distance_m;
CREATE UNIQUE INDEX IF NOT EXISTS {table}_idx ON  {table} ({id}); 
'''.format(id = points_id.lower(),table = 'nh_inds_distance')
curs.execute(nh_distance)
conn.commit()
print("Done.")

print("Create hard and soft threshold indicators for curated destination categories...")
for threshold_type in ['hard','soft']:
    for nh_threshold in [400,800,1000,1600]:
        print("  - nh_inds_{threshold_type}_{nh_threshold}m".format(threshold_type = threshold_type, nh_threshold = nh_threshold))
        sql = '''
        DROP TABLE IF EXISTS nh_inds_{threshold_type}_{nh_threshold}m;
        CREATE TABLE IF NOT EXISTS nh_inds_{threshold_type}_{nh_threshold}m AS
        SELECT  
        {id},
        threshold_{threshold_type}(activity_centres_hlc_2017     ,{nh_threshold}) AS activity_centres_hlc_2017     , 
        threshold_{threshold_type}(convenience_osm_2018          ,{nh_threshold}) AS convenience_osm_2018          , 
        threshold_{threshold_type}(supermarket_hlc_2017          ,{nh_threshold}) AS supermarket_hlc_2017          , 
        threshold_{threshold_type}(supermarket_osm_2018          ,{nh_threshold}) AS supermarket_osm_2018          , 
        threshold_{threshold_type}(pt_any_gtfs_hlc_2018          ,{nh_threshold}) AS pt_any_gtfs_hlc_2018          ,
        threshold_{threshold_type}(pt_freq_gtfs_hlc_2018         ,{nh_threshold}) AS pt_freq_gtfs_hlc_2018         ,
        threshold_{threshold_type}(childcare_meets_acequa_2019   ,{nh_threshold}) AS childcare_meets_acequa_2019   ,
        threshold_{threshold_type}(primary_school_acara_2017     ,{nh_threshold}) AS primary_school_acara_2017     ,
        threshold_{threshold_type}(secondary_school_acara_2017   ,{nh_threshold}) AS secondary_school_acara_2017   ,
        threshold_{threshold_type}(community_pow_osm_2018        ,{nh_threshold}) AS community_pow_osm_2018        ,
        threshold_{threshold_type}(libraries_hlc_2018            ,{nh_threshold}) AS libraries_hlc_2018            ,
        threshold_{threshold_type}(postoffice_osm_2018           ,{nh_threshold}) AS postoffice_osm_2018           ,
        threshold_{threshold_type}(dentist_nhsd_2017             ,{nh_threshold}) AS dentist_nhsd_2017             ,
        threshold_{threshold_type}(pharmacy_nhsd_2017            ,{nh_threshold}) AS pharmacy_nhsd_2017            ,
        threshold_{threshold_type}(gp_nhsd_2017                  ,{nh_threshold}) AS gp_nhsd_2017                  ,
        threshold_{threshold_type}(food_fresh_specialty_osm_2018 ,{nh_threshold}) AS food_fresh_specialty_osm_2018 ,
        threshold_{threshold_type}(food_fast_hlc_2017            ,{nh_threshold}) AS food_fast_hlc_2017            ,
        threshold_{threshold_type}(food_fast_osm_2018            ,{nh_threshold}) AS food_fast_osm_2018            ,
        threshold_{threshold_type}(food_dining_osm_2018          ,{nh_threshold}) AS food_dining_osm_2018          ,
        threshold_{threshold_type}(culture_osm_2018              ,{nh_threshold}) AS culture_osm_2018              ,
        threshold_{threshold_type}(alcohol_nightlife_osm_2018    ,{nh_threshold}) AS alcohol_nightlife_osm_2018    ,
        threshold_{threshold_type}(alcohol_offlicence_osm_2018   ,{nh_threshold}) AS alcohol_offlicence_osm_2018   ,
        threshold_{threshold_type}(alcohol_offlicence_hlc_2017_19,{nh_threshold}) AS alcohol_offlicence_hlc_2017_19,
        threshold_{threshold_type}(alcohol_onlicence_hlc_2017_19 ,{nh_threshold}) AS alcohol_onlicence_hlc_2017_19 ,  
        threshold_{threshold_type}(tobacco_osm_2018              ,{nh_threshold}) AS tobacco_osm_2018              ,
        threshold_{threshold_type}(gambling_osm_2018             ,{nh_threshold}) AS gambling_osm_2018           
        FROM nh_inds_distance ;
        CREATE UNIQUE INDEX IF NOT EXISTS nh_inds_{threshold_type}_{nh_threshold}m_idx ON  nh_inds_{threshold_type}_{nh_threshold}m ({id}); 
        '''.format(id = points_id.lower(),threshold_type = threshold_type, nh_threshold = nh_threshold)
        curs.execute(sql)
        conn.commit()
print("Done.")

# print("Create ISO37120 indicator (hard threshold is native version; soft threshold is novel...")
# to do... could base on the nh_inds with specific thresholds

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
