# Script:  17_aedc_measures.py
# Purpose: Create parcel indicators for national liveability project
# Author:  Carl Higgs 
# Date:    20180717

import time
import psycopg2 
from sqlalchemy import create_engine

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

# Get a list of destinations processed within this region for distance to closest
sql = '''SELECT DISTINCT(dest_name) dest_name FROM od_closest ORDER BY dest_name;'''
curs.execute(sql)
categories = [x[0] for x in curs.fetchall()]
category_list = ','.join(categories)
category_types = '"{}" int'.format('" int, "'.join(categories))
sql = '''SELECT DISTINCT(dest_class) FROM od_distances_3200m ORDER BY dest_class;'''
curs.execute(sql)
array_categories = [x[0] for x in curs.fetchall()]
array_category_list = ','.join(array_categories)
array_category_types = '"{}" int[]'.format('" int[], "'.join(array_categories))

print("Create summary table of destination distances... "),
crosstab = '''
DROP TABLE IF EXISTS dest_distance_m;
CREATE TABLE dest_distance_m AS
SELECT *
  FROM   crosstab(
   'SELECT gnaf_pid, dest_name, distance
    FROM   od_closest
    ORDER  BY 1,2'  -- could also just be "ORDER BY 1" here
  ,$$SELECT unnest('{curly_o}{category_list}{curly_c}'::text[])$$
   ) AS distance ("gnaf_pid" text, {category_types});
'''.format(id = points_id.lower(),
           curly_o = "{",
           curly_c = "}",
           category_list = category_list,
           category_types = category_types)
curs.execute(crosstab)
conn.commit()
print("Done.")

print("Create summary table of destination distance arrays... "),
crosstab = '''
DROP TABLE IF EXISTS dest_distances_3200m;
CREATE TABLE dest_distances_3200m AS
SELECT *
  FROM   crosstab(
   'SELECT gnaf_pid, dest_class, distances
    FROM   od_distances_3200m
    ORDER  BY 1,2'  -- could also just be "ORDER BY 1" here
  ,$$SELECT unnest('{curly_o}{category_list}{curly_c}'::text[])$$
   ) AS distances ("gnaf_pid" text, {category_types});
'''.format(id = points_id.lower(),
           curly_o = "{",
           curly_c = "}",
           category_list = array_category_list,
           category_types = array_category_types)
curs.execute(crosstab)
conn.commit()
print("Done.")

aedc_measures = '''
DROP TABLE IF EXISTS aedc_measures;
CREATE TABLE aedc_measures AS
SELECT
p.gnaf_pid               ,
p.count_objectid         ,
p.point_x                ,
p.point_y                ,
p.hex_id                 ,
abs.mb_code_2016         ,
abs.mb_category_name_2016,
abs.dwelling             ,
abs.person               ,
abs.sa1_maincode         ,
abs.sa2_name_2016        ,
abs.sa3_name_2016        ,
abs.sa4_name_2016        ,
abs.gccsa_name           ,
abs.state_name           ,
non_abs.ssc_code_2016    ,
non_abs.ssc_name_2016    ,
non_abs.lga_code_2016    ,
non_abs.lga_name_2016    ,
sos.sos_name_2016        ,
ind_activity.distance/1000.0 AS walk_12,
sc_nh1600m.sc_nh1600m AS walk_15,
dd_nh1600m.dd_nh1600m AS walk_16,
ind_walkability.wa_soft AS walk_17_soft,
ind_walkability.wa_hard AS walk_17_hard,
dest_distance_m.dist_cl_alcohol_offlicence,
dest_distance_m.dist_cl_alcohol_onlicence,
dest_distance_m.dist_cl_conveniencestores_2014,
dest_distance_m.dist_cl_newsagents_2014,
dest_distance_m.dist_cl_petrolstations_2014,
dest_distance_m.dist_cl_fastfood_2017,
dest_distance_m.dist_cl_supermarkets_2017,
dest_distance_m.dist_cl_activity_centres_2017,
dest_distance_m.dist_cl_childcare2018,
dest_distance_m.dist_cl_childcare_oshc_2018,
dest_distance_m.dist_cl_preschool_childcare_2018,
dest_distance_m.dist_cl_supermarket_osm,
dest_distance_m.dist_cl_bakery_osm,
dest_distance_m.dist_cl_meat_seafood_osm,
dest_distance_m.dist_cl_fruit_veg_osm,
dest_distance_m.dist_cl_deli_osm,
dest_distance_m.dist_cl_convenience_osm,
dest_distance_m.dist_cl_petrolstation_osm,
dest_distance_m.dist_cl_newsagent_osm,
dest_distance_m.dist_cl_food_other_osm,
dest_distance_m.dist_cl_food_health_osm,
dest_distance_m.dist_cl_market_osm,
dest_distance_m.dist_cl_community_centre_osm,
dest_distance_m.dist_cl_place_of_worship_osm,
dest_distance_m.dist_cl_museum_osm,
dest_distance_m.dist_cl_theatre_osm,
dest_distance_m.dist_cl_cinema_osm,
dest_distance_m.dist_cl_art_gallery_osm,
dest_distance_m.dist_cl_art_centre_osm,
dest_distance_m.dist_cl_artwork_osm,
dest_distance_m.dist_cl_fountain_osm,
dest_distance_m.dist_cl_viewpoint_osm,
dest_distance_m.dist_cl_picnic_site_osm,
dest_distance_m.dist_cl_pharmacy_osm,
dest_distance_m.dist_cl_restaurant_osm,
dest_distance_m.dist_cl_cafe_osm,
dest_distance_m.dist_cl_eatery_osm,
dest_distance_m.dist_cl_food_court_osm,
dest_distance_m.dist_cl_fastfood_osm,
dest_distance_m.dist_cl_pub_osm,
dest_distance_m.dist_cl_bar_osm,
dest_distance_m.dist_cl_nightclub_osm,
dest_distance_m.dist_cl_gambling_osm,
dest_distance_m.dist_cl_alcohol_osm,
dest_distance_m.dist_cl_tobacco_osm,
dest_distance_m.dist_cl_swimming_pool_osm,
dest_distance_m.dist_cl_all_schools2018,
dest_distance_m.dist_cl_P_12_Schools2018,
dest_distance_m.dist_cl_primary_schools2018,
dest_distance_m.dist_cl_secondary_schools2018,
dest_distance_m.dist_cl_special_schools2018,
dest_distance_m.dist_cl_libraries_2018,
dest_distance_m.dist_cl_gtfs_2018_stop_30_mins_final,
dest_distance_m.dist_cl_gtfs_2018_stops,
dest_distance_m.dist_cl_gtfs_2018_stops_bus,
dest_distance_m.dist_cl_gtfs_2018_stops_ferry,
dest_distance_m.dist_cl_gtfs_2018_stops_train,
dest_distance_m.dist_cl_gtfs_2018_stops_tram,
dest_distance_m.dist_cl_gtfs_nsw_stop_30_mins_bus,
dest_distance_m.dist_cl_gtfs_nsw_stop_15_mins_train,
dest_distance_m.dist_cl_Hospital,
dest_distance_m.dist_cl_PhysicalActivity_Recreation,
dest_distance_m.dist_cl_ChildcareKinder_LongDayChildCare,
dest_distance_m.dist_cl_ChildcareKinder_Kinder_Preschool,
dest_distance_m.dist_cl_ChildcareKinder_HolidayProgram,
dest_distance_m.dist_cl_ChildcareKinder_KinderDisability,
dest_distance_m.dist_cl_ChildcareKinder_OSHC,
dest_distance_m.dist_cl_ChildcareKinder_OccasionalCare,
dest_distance_m.dist_cl_ChildcareKinder_FamilyDayCare,
dest_distance_m.dist_cl_ChildDevelopment_Playgroup,
dest_distance_m.dist_cl_ChildDevelopment_ParentingFamilySupport,
dest_distance_m.dist_cl_ChildDevelopment_ChildPlayProgram,
dest_distance_m.dist_cl_ChildDevelopment_EarlyParentingSupport,
dest_distance_m.dist_cl_ChildDevelopment_ToyLibrary,
dest_distance_m.dist_cl_ChildDevelopment_SchoolNursing,
dest_distance_m.dist_cl_ChildProtectionFamilyServices_Integrated,
dest_distance_m.dist_cl_CommunityHealthCare_Pharmacy,
dest_distance_m.dist_cl_CommunityHealthCare_MCH,
dest_distance_m.dist_cl_CommunityHealthCare_Immunisation,
dest_distance_m.dist_cl_Counselling_CounsellingFamilyTherapy,
dest_distance_m.dist_cl_Counselling_GeneralCounselling,
dest_distance_m.dist_cl_DisabilitySupport_EarlyChildhoodIntervention,
dest_distance_m.dist_cl_EducationLearning_Library,
dest_distance_m.dist_cl_GeneralPracticeGP_GP,
dest_distance_m.dist_cl_MentalHealth_ChildMentalHealth,
dest_distance_m.dist_cl_SpecialistPaediatric_PaediatricMedicine,
dest_distance_m.dist_cl_MentalHealth_GeneralMentalHealthService,
dest_distance_m.dist_cl_MentalHealth_AdultMentalHealthService,
dest_distance_m.dist_cl_MentalHealth_Psychology,
dest_distance_m.dist_cl_toilets_2018,
dest_distance_m.dist_cl_playgrounds_2018,
dest_distance_m.dist_cl_centrelink_2018,
dest_distances_3200m.dist_3200m_alcohol_offlicence,
dest_distances_3200m.dist_3200m_alcohol_onlicence,
dest_distances_3200m.dist_3200m_convenience,
dest_distances_3200m.dist_3200m_newsagent,
dest_distances_3200m.dist_3200m_petrolstation,
dest_distances_3200m.dist_3200m_fast_food,
dest_distances_3200m.dist_3200m_supermarket,
dest_distances_3200m.dist_3200m_activity_centres,
dest_distances_3200m.dist_3200m_childcare_all,
dest_distances_3200m.dist_3200m_childcare_oshc,
dest_distances_3200m.dist_3200m_childcare_preschool,
dest_distances_3200m.dist_3200m_supermarket_osm,
dest_distances_3200m.dist_3200m_bakery_osm,
dest_distances_3200m.dist_3200m_meat_seafood_osm,
dest_distances_3200m.dist_3200m_fruit_veg_osm,
dest_distances_3200m.dist_3200m_deli_osm,
dest_distances_3200m.dist_3200m_convenience_osm,
dest_distances_3200m.dist_3200m_petrolstation_osm,
dest_distances_3200m.dist_3200m_newsagent_osm,
dest_distances_3200m.dist_3200m_food_other_osm,
dest_distances_3200m.dist_3200m_food_health_osm,
dest_distances_3200m.dist_3200m_market_osm,
dest_distances_3200m.dist_3200m_community_centre_osm,
dest_distances_3200m.dist_3200m_place_of_worship_osm,
dest_distances_3200m.dist_3200m_museum_osm,
dest_distances_3200m.dist_3200m_theatre_osm,
dest_distances_3200m.dist_3200m_cinema_osm,
dest_distances_3200m.dist_3200m_art_gallery_osm,
dest_distances_3200m.dist_3200m_art_centre_osm,
dest_distances_3200m.dist_3200m_artwork_osm,
dest_distances_3200m.dist_3200m_fountain_osm,
dest_distances_3200m.dist_3200m_viewpoint_osm,
dest_distances_3200m.dist_3200m_picnic_site_osm,
dest_distances_3200m.dist_3200m_pharmacy_osm,
dest_distances_3200m.dist_3200m_restaurant_osm,
dest_distances_3200m.dist_3200m_cafe_osm,
dest_distances_3200m.dist_3200m_eatery_osm,
dest_distances_3200m.dist_3200m_food_court_osm,
dest_distances_3200m.dist_3200m_fastfood_osm,
dest_distances_3200m.dist_3200m_pub_osm,
dest_distances_3200m.dist_3200m_bar_osm,
dest_distances_3200m.dist_3200m_nightclub_osm,
dest_distances_3200m.dist_3200m_gambling_osm,
dest_distances_3200m.dist_3200m_alcohol_osm,
dest_distances_3200m.dist_3200m_tobacco_osm,
dest_distances_3200m.dist_3200m_swimming_pool_osm,
dest_distances_3200m.dist_3200m_all_schools,
dest_distances_3200m.dist_3200m_P_12_Schools,
dest_distances_3200m.dist_3200m_primary_schools,
dest_distances_3200m.dist_3200m_secondary_schools,
dest_distances_3200m.dist_3200m_special_schools,
dest_distances_3200m.dist_3200m_libraries,
dest_distances_3200m.dist_3200m_gtfs_2018_stop_30_mins_final,
dest_distances_3200m.dist_3200m_gtfs_2018_stops,
dest_distances_3200m.dist_3200m_gtfs_2018_stops_bus,
dest_distances_3200m.dist_3200m_gtfs_2018_stops_ferry,
dest_distances_3200m.dist_3200m_gtfs_2018_stops_train,
dest_distances_3200m.dist_3200m_gtfs_2018_stops_tram,
dest_distances_3200m.dist_3200m_gtfs_nsw_stop_30_mins_bus,
dest_distances_3200m.dist_3200m_gtfs_nsw_stop_15_mins_train,
dest_distances_3200m.dist_3200m_Hospital,
dest_distances_3200m.dist_3200m_PhysicalActivity_Recreation,
dest_distances_3200m.dist_3200m_ChildcareKinder_LongDayChildCare,
dest_distances_3200m.dist_3200m_ChildcareKinder_Kinder_Preschool,
dest_distances_3200m.dist_3200m_ChildcareKinder_HolidayProgram,
dest_distances_3200m.dist_3200m_ChildcareKinder_KinderDisability,
dest_distances_3200m.dist_3200m_ChildcareKinder_OSHC,
dest_distances_3200m.dist_3200m_ChildcareKinder_OccasionalCare,
dest_distances_3200m.dist_3200m_ChildcareKinder_FamilyDayCare,
dest_distances_3200m.dist_3200m_ChildDevelopment_Playgroup,
dest_distances_3200m.dist_3200m_ChildDevelopment_ParentingFamilySupport,
dest_distances_3200m.dist_3200m_ChildDevelopment_ChildPlayProgram,
dest_distances_3200m.dist_3200m_ChildDevelopment_EarlyParentingSupport,
dest_distances_3200m.dist_3200m_ChildDevelopment_ToyLibrary,
dest_distances_3200m.dist_3200m_ChildDevelopment_SchoolNursing,
dest_distances_3200m.dist_3200m_ChildProtectionFamilyServices_Integrated,
dest_distances_3200m.dist_3200m_CommunityHealthCare_Pharmacy,
dest_distances_3200m.dist_3200m_CommunityHealthCare_MCH,
dest_distances_3200m.dist_3200m_CommunityHealthCare_Immunisation,
dest_distances_3200m.dist_3200m_Counselling_CounsellingFamilyTherapy,
dest_distances_3200m.dist_3200m_Counselling_GeneralCounselling,
dest_distances_3200m.dist_3200m_DisabilitySupport_EarlyChildhoodIntervention,
dest_distances_3200m.dist_3200m_EducationLearning_Library,
dest_distances_3200m.dist_3200m_GeneralPracticeGP_GP,
dest_distances_3200m.dist_3200m_MentalHealth_ChildMentalHealth,
dest_distances_3200m.dist_3200m_SpecialistPaediatric_PaediatricMedicine,
dest_distances_3200m.dist_3200m_MentalHealth_GeneralMentalHealthService,
dest_distances_3200m.dist_3200m_MentalHealth_AdultMentalHealthService,
dest_distances_3200m.dist_3200m_MentalHealth_Psychology,
dest_distances_3200m.dist_3200m_toilets,
dest_distances_3200m.dist_3200m_playgrounds,
dest_distances_3200m.dist_3200m_centrelink,
od_aos_jsonb.attributes AS aos_distances,
p.geom
FROM
parcel_dwellings p
LEFT JOIN abs_linkage abs ON p.mb_code_20 = abs.mb_code_2016
LEFT JOIN non_abs_linkage non_abs ON p.gnaf_pid = non_abs.gnaf_pid
LEFT JOIN parcel_sos sos ON p.gnaf_pid = sos.gnaf_pid
LEFT JOIN ind_activity ON p.gnaf_pid = ind_activity.gnaf_pid
LEFT JOIN sc_nh1600m ON p.gnaf_pid = sc_nh1600m.gnaf_pid
LEFT JOIN dd_nh1600m ON p.gnaf_pid = dd_nh1600m.gnaf_pid
LEFT JOIN ind_walkability ON p.gnaf_pid = ind_walkability.gnaf_pid
LEFT JOIN ind_transport ON p.gnaf_pid = ind_transport.gnaf_pid
LEFT JOIN ind_pos_closest ON p.gnaf_pid = ind_pos_closest.gnaf_pid
LEFT JOIN od_aos_jsonb ON p.gnaf_pid = od_aos_jsonb
'''
curs.execute(aedc_measures)
conn.commit(aedc_measures)

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
