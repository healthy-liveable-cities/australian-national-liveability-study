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
CREATE TABLE IF NOT EXISTS dest_distance_m AS
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

print("Ensure dest distance table has all required fields (you may have to edit the script on from line 57!"),
required_dest_names = '''
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS alcohol_offlicence int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS alcohol_onlicence int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS conveniencestores_2014 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS newsagents_2014 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS petrolstations_2014 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS fastfood_2017 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS supermarkets_2017 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS activity_centres_2017 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS childcare_2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS childcare_meet_2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS childcare_exc_2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS childcare_oshc_2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS childcare_oshc_meet_2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS childcare_oshc_exc_2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS preschool_childcare_2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS preschool_childcare_meet_2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS preschool_childcare_exc_2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS supermarket_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS bakery_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS meat_seafood_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS fruit_veg_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS deli_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS convenience_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS petrolstation_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS newsagent_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS food_other_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS food_health_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS market_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS community_centre_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS place_of_worship_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS museum_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS theatre_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS cinema_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS art_gallery_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS art_centre_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS artwork_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS fountain_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS viewpoint_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS picnic_site_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS pharmacy_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS restaurant_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS cafe_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS eatery_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS food_court_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS fastfood_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS pub_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS bar_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS nightclub_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS gambling_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS alcohol_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS tobacco_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS swimming_pool_osm int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS all_schools2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS P_12_Schools2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS primary_schools2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS secondary_schools2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS special_schools2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS libraries_2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS gtfs_2018_stop_30_mins_final int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS gtfs_2018_stops int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS gtfs_2018_stops_bus int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS gtfs_2018_stops_ferry int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS gtfs_2018_stops_train int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS gtfs_2018_stops_tram int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS gtfs_nsw_stop_30_mins_bus int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS gtfs_nsw_stop_15_mins_train int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS Hospital int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS PhysicalActivity_Recreation int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildcareKinder_LongDayChildCare int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildcareKinder_Kinder_Preschool int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildcareKinder_HolidayProgram int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildcareKinder_KinderDisability int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildcareKinder_OSHC int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildcareKinder_OccasionalCare int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildcareKinder_FamilyDayCare int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildDevelopment_Playgroup int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildDevelopment_ParentingFamilySupport int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildDevelopment_ChildPlayProgram int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildDevelopment_EarlyParentingSupport int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildDevelopment_ToyLibrary int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildDevelopment_SchoolNursing int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS ChildProtectionFamilyServices_Integrated int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS CommunityHealthCare_Pharmacy int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS CommunityHealthCare_MCH int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS CommunityHealthCare_Immunisation int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS Counselling_CounsellingFamilyTherapy int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS Counselling_GeneralCounselling int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS DisabilitySupport_EarlyChildhoodIntervention int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS EducationLearning_Library int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS GeneralPracticeGP_GP int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS MentalHealth_ChildMentalHealth int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS SpecialistPaediatric_PaediatricMedicine int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS MentalHealth_GeneralMentalHealthService int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS MentalHealth_AdultMentalHealthService int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS MentalHealth_Psychology int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS toilets_2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS playgrounds_2018 int ;
ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS centrelink_2018 int ;
'''
curs.execute(required_dest_names)
conn.commit()
print("Done.")

print("Create summary table of destination distance arrays... "),
crosstab = '''
DROP TABLE IF EXISTS dest_distances_3200m;
CREATE TABLE IF NOT EXISTS dest_distances_3200m AS
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

print("Ensure dest distances 3200 table has all required fields (you may have to edit the script on from line 176!"),
required_dest_classes = '''
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS alcohol_offlicence int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS alcohol_onlicence int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS convenience int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS newsagent int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS petrolstation int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS fast_food int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS supermarket int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS activity_centres int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS childcare_all int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS childcare_all_meet int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS childcare_all_exc int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS childcare_oshc int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS childcare_oshc_meet int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS childcare_oshc_exc int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS childcare_preschool int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS childcare_preschool_meet int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS childcare_preschool_exc int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS supermarket_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS bakery_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS meat_seafood_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS fruit_veg_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS deli_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS convenience_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS petrolstation_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS newsagent_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS food_other_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS food_health_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS market_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS community_centre_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS place_of_worship_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS museum_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS theatre_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS cinema_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS art_gallery_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS art_centre_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS artwork_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS fountain_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS viewpoint_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS picnic_site_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS pharmacy_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS restaurant_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS cafe_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS eatery_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS food_court_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS fastfood_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS pub_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS bar_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS nightclub_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS gambling_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS alcohol_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS tobacco_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS swimming_pool_osm int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS all_schools int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS P_12_Schools int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS primary_schools int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS secondary_schools int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS special_schools int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS libraries int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS gtfs_2018_stop_30_mins_final int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS gtfs_2018_stops int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS gtfs_2018_stops_bus int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS gtfs_2018_stops_ferry int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS gtfs_2018_stops_train int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS gtfs_2018_stops_tram int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS gtfs_nsw_stop_30_mins_bus int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS gtfs_nsw_stop_15_mins_train int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS Hospital int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS PhysicalActivity_Recreation int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildcareKinder_LongDayChildCare int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildcareKinder_Kinder_Preschool int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildcareKinder_HolidayProgram int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildcareKinder_KinderDisability int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildcareKinder_OSHC int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildcareKinder_OccasionalCare int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildcareKinder_FamilyDayCare int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildDevelopment_Playgroup int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildDevelopment_ParentingFamilySupport int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildDevelopment_ChildPlayProgram int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildDevelopment_EarlyParentingSupport int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildDevelopment_ToyLibrary int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildDevelopment_SchoolNursing int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS ChildProtectionFamilyServices_Integrated int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS CommunityHealthCare_Pharmacy int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS CommunityHealthCare_MCH int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS CommunityHealthCare_Immunisation int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS Counselling_CounsellingFamilyTherapy int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS Counselling_GeneralCounselling int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS DisabilitySupport_EarlyChildhoodIntervention int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS EducationLearning_Library int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS GeneralPracticeGP_GP int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS MentalHealth_ChildMentalHealth int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS SpecialistPaediatric_PaediatricMedicine int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS MentalHealth_GeneralMentalHealthService int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS MentalHealth_AdultMentalHealthService int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS MentalHealth_Psychology int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS toilets int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS playgrounds int[] ;
ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS centrelink int[] ;
'''
curs.execute(required_dest_classes)
conn.commit()
print("Done.")

aedc_measures = '''
DROP TABLE IF EXISTS aedc_measures;
CREATE TABLE aedc_measures AS
SELECT
p.gnaf_pid               ,
'{locale}' AS locale,
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
dest_distance_m.conveniencestores_2014 AS dist_cl_conveniencestores_2014,
dest_distance_m.newsagents_2014 AS dist_cl_newsagents_2014,
dest_distance_m.petrolstations_2014 AS dist_cl_petrolstations_2014,
dest_distance_m.fastfood_2017 AS dist_cl_fastfood_2017,
dest_distance_m.supermarkets_2017 AS dist_cl_supermarkets_2017,
dest_distance_m.activity_centres_2017 AS dist_cl_activity_centres_2017,
dest_distance_m.supermarket_osm AS dist_cl_supermarket_osm,
dest_distance_m.bakery_osm AS dist_cl_bakery_osm,
dest_distance_m.meat_seafood_osm AS dist_cl_meat_seafood_osm,
dest_distance_m.fruit_veg_osm AS dist_cl_fruit_veg_osm,
dest_distance_m.deli_osm AS dist_cl_deli_osm,
dest_distance_m.convenience_osm AS dist_cl_convenience_osm,
dest_distance_m.petrolstation_osm AS dist_cl_petrolstation_osm,
dest_distance_m.newsagent_osm AS dist_cl_newsagent_osm,
dest_distance_m.food_other_osm AS dist_cl_food_other_osm,
dest_distance_m.food_health_osm AS dist_cl_food_health_osm,
dest_distance_m.market_osm AS dist_cl_market_osm,
dest_distance_m.community_centre_osm AS dist_cl_community_centre_osm,
dest_distance_m.place_of_worship_osm AS dist_cl_place_of_worship_osm,
dest_distance_m.museum_osm AS dist_cl_museum_osm,
dest_distance_m.theatre_osm AS dist_cl_theatre_osm,
dest_distance_m.cinema_osm AS dist_cl_cinema_osm,
dest_distance_m.art_gallery_osm AS dist_cl_art_gallery_osm,
dest_distance_m.art_centre_osm AS dist_cl_art_centre_osm,
dest_distance_m.artwork_osm AS dist_cl_artwork_osm,
dest_distance_m.fountain_osm AS dist_cl_fountain_osm,
dest_distance_m.viewpoint_osm AS dist_cl_viewpoint_osm,
dest_distance_m.picnic_site_osm AS dist_cl_picnic_site_osm,
dest_distance_m.pharmacy_osm AS dist_cl_pharmacy_osm,
dest_distance_m.restaurant_osm AS dist_cl_restaurant_osm,
dest_distance_m.cafe_osm AS dist_cl_cafe_osm,
dest_distance_m.eatery_osm AS dist_cl_eatery_osm,
dest_distance_m.food_court_osm AS dist_cl_food_court_osm,
dest_distance_m.fastfood_osm AS dist_cl_fastfood_osm,
dest_distance_m.pub_osm AS dist_cl_pub_osm,
dest_distance_m.bar_osm AS dist_cl_bar_osm,
dest_distance_m.nightclub_osm AS dist_cl_nightclub_osm,
dest_distance_m.gambling_osm AS dist_cl_gambling_osm,
dest_distance_m.alcohol_osm AS dist_cl_alcohol_osm,
dest_distance_m.tobacco_osm AS dist_cl_tobacco_osm,
dest_distance_m.swimming_pool_osm AS dist_cl_swimming_pool_osm,
dest_distance_m.all_schools2018 AS dist_cl_all_schools2018,
dest_distance_m.P_12_Schools2018 AS dist_cl_P_12_Schools2018,
dest_distance_m.primary_schools2018 AS dist_cl_primary_schools2018,
dest_distance_m.secondary_schools2018 AS dist_cl_secondary_schools2018,
dest_distance_m.special_schools2018 AS dist_cl_special_schools2018,
dest_distance_m.libraries_2018 AS dist_cl_libraries_2018,
dest_distance_m.gtfs_2018_stop_30_mins_final AS dist_cl_gtfs_2018_stop_30_mins_final,
dest_distance_m.gtfs_2018_stops AS dist_cl_gtfs_2018_stops,
dest_distance_m.gtfs_2018_stops_bus AS dist_cl_gtfs_2018_stops_bus,
dest_distance_m.gtfs_2018_stops_ferry AS dist_cl_gtfs_2018_stops_ferry,
dest_distance_m.gtfs_2018_stops_train AS dist_cl_gtfs_2018_stops_train,
dest_distance_m.gtfs_2018_stops_tram AS dist_cl_gtfs_2018_stops_tram,
dest_distance_m.gtfs_nsw_stop_30_mins_bus AS dist_cl_gtfs_nsw_stop_30_mins_bus,
dest_distance_m.gtfs_nsw_stop_15_mins_train AS dist_cl_gtfs_nsw_stop_15_mins_train,
dest_distance_m.Hospital AS dist_cl_Hospital,
dest_distance_m.PhysicalActivity_Recreation AS dist_cl_PhysicalActivity_Recreation,
dest_distance_m.ChildcareKinder_LongDayChildCare AS dist_cl_ChildcareKinder_LongDayChildCare,
dest_distance_m.ChildcareKinder_Kinder_Preschool AS dist_cl_ChildcareKinder_Kinder_Preschool,
dest_distance_m.ChildcareKinder_HolidayProgram AS dist_cl_ChildcareKinder_HolidayProgram,
dest_distance_m.ChildcareKinder_KinderDisability AS dist_cl_ChildcareKinder_KinderDisability,
dest_distance_m.ChildcareKinder_OSHC AS dist_cl_ChildcareKinder_OSHC,
dest_distance_m.ChildcareKinder_OccasionalCare AS dist_cl_ChildcareKinder_OccasionalCare,
dest_distance_m.ChildcareKinder_FamilyDayCare AS dist_cl_ChildcareKinder_FamilyDayCare,
dest_distance_m.ChildDevelopment_Playgroup AS dist_cl_ChildDevelopment_Playgroup,
dest_distance_m.ChildDevelopment_ParentingFamilySupport AS dist_cl_ChildDevelopment_ParentingFamilySupport,
dest_distance_m.ChildDevelopment_ChildPlayProgram AS dist_cl_ChildDevelopment_ChildPlayProgram,
dest_distance_m.ChildDevelopment_EarlyParentingSupport AS dist_cl_ChildDevelopment_EarlyParentingSupport,
dest_distance_m.ChildDevelopment_ToyLibrary AS dist_cl_ChildDevelopment_ToyLibrary,
dest_distance_m.ChildDevelopment_SchoolNursing AS dist_cl_ChildDevelopment_SchoolNursing,
dest_distance_m.ChildProtectionFamilyServices_Integrated AS dist_cl_ChildProtectionFamilyServices_Integrated,
dest_distance_m.CommunityHealthCare_Pharmacy AS dist_cl_CommunityHealthCare_Pharmacy,
dest_distance_m.CommunityHealthCare_MCH AS dist_cl_CommunityHealthCare_MCH,
dest_distance_m.CommunityHealthCare_Immunisation AS dist_cl_CommunityHealthCare_Immunisation,
dest_distance_m.Counselling_CounsellingFamilyTherapy AS dist_cl_Counselling_CounsellingFamilyTherapy,
dest_distance_m.Counselling_GeneralCounselling AS dist_cl_Counselling_GeneralCounselling,
dest_distance_m.DisabilitySupport_EarlyChildhoodIntervention AS dist_cl_DisabilitySupport_EarlyChildhoodIntervention,
dest_distance_m.EducationLearning_Library AS dist_cl_EducationLearning_Library,
dest_distance_m.GeneralPracticeGP_GP AS dist_cl_GeneralPracticeGP_GP,
dest_distance_m.MentalHealth_ChildMentalHealth AS dist_cl_MentalHealth_ChildMentalHealth,
dest_distance_m.SpecialistPaediatric_PaediatricMedicine AS dist_cl_SpecialistPaediatric_PaediatricMedicine,
dest_distance_m.MentalHealth_GeneralMentalHealthService AS dist_cl_MentalHealth_GeneralMentalHealthService,
dest_distance_m.MentalHealth_AdultMentalHealthService AS dist_cl_MentalHealth_AdultMentalHealthService,
dest_distance_m.MentalHealth_Psychology AS dist_cl_MentalHealth_Psychology,
dest_distance_m.toilets_2018 AS dist_cl_toilets_2018,
dest_distance_m.playgrounds_2018 AS dist_cl_playgrounds_2018,
dest_distance_m.centrelink_2018 AS dist_cl_centrelink_2018,
-- you could add in new destinations here; be sure to remember to end the line(s) with a comma!! eg.
-- dest_distance_m.destination AS dist_cl_destination,
-- Please uncomment below for alchol!!
dest_distance_m.alcohol_offlicence AS dist_cl_alcohol_offlicence,
dest_distance_m.alcohol_onlicence AS dist_cl_alcohol_onlicence,
dest_distance_m.childcare_2018 AS dist_cl_childcare_2018,
dest_distance_m.childcare_meet_2018 AS dist_cl_childcare_meet_2018,
dest_distance_m.childcare_exc_2018 AS dist_cl_childcare_exc_2018,
dest_distance_m.childcare_oshc_2018 AS dist_cl_childcare_oshc_2018,
dest_distance_m.childcare_oshc_meet_2018 AS dist_cl_childcare_oshc_meet_2018,
dest_distance_m.childcare_oshc_exc_2018 AS dist_cl_childcare_oshc_exc_2018,
dest_distance_m.preschool_childcare_2018 AS dist_cl_preschool_childcare_2018,
dest_distance_m.preschool_childcare_meet_2018 AS dist_cl_preschool_childcare_meet_2018,
dest_distance_m.preschool_childcare_exc_2018 AS dist_cl_preschool_childcare_exc_2018,
dest_distances_3200m.convenience AS dist_3200m_convenience,
dest_distances_3200m.newsagent AS dist_3200m_newsagent,
dest_distances_3200m.petrolstation AS dist_3200m_petrolstation,
dest_distances_3200m.fast_food AS dist_3200m_fast_food,
dest_distances_3200m.supermarket AS dist_3200m_supermarket,
dest_distances_3200m.activity_centres AS dist_3200m_activity_centres,
dest_distances_3200m.supermarket_osm AS dist_3200m_supermarket_osm,
dest_distances_3200m.bakery_osm AS dist_3200m_bakery_osm,
dest_distances_3200m.meat_seafood_osm AS dist_3200m_meat_seafood_osm,
dest_distances_3200m.fruit_veg_osm AS dist_3200m_fruit_veg_osm,
dest_distances_3200m.deli_osm AS dist_3200m_deli_osm,
dest_distances_3200m.convenience_osm AS dist_3200m_convenience_osm,
dest_distances_3200m.petrolstation_osm AS dist_3200m_petrolstation_osm,
dest_distances_3200m.newsagent_osm AS dist_3200m_newsagent_osm,
dest_distances_3200m.food_other_osm AS dist_3200m_food_other_osm,
dest_distances_3200m.food_health_osm AS dist_3200m_food_health_osm,
dest_distances_3200m.market_osm AS dist_3200m_market_osm,
dest_distances_3200m.community_centre_osm AS dist_3200m_community_centre_osm,
dest_distances_3200m.place_of_worship_osm AS dist_3200m_place_of_worship_osm,
dest_distances_3200m.museum_osm AS dist_3200m_museum_osm,
dest_distances_3200m.theatre_osm AS dist_3200m_theatre_osm,
dest_distances_3200m.cinema_osm AS dist_3200m_cinema_osm,
dest_distances_3200m.art_gallery_osm AS dist_3200m_art_gallery_osm,
dest_distances_3200m.art_centre_osm AS dist_3200m_art_centre_osm,
dest_distances_3200m.artwork_osm AS dist_3200m_artwork_osm,
dest_distances_3200m.fountain_osm AS dist_3200m_fountain_osm,
dest_distances_3200m.viewpoint_osm AS dist_3200m_viewpoint_osm,
dest_distances_3200m.picnic_site_osm AS dist_3200m_picnic_site_osm,
dest_distances_3200m.pharmacy_osm AS dist_3200m_pharmacy_osm,
dest_distances_3200m.restaurant_osm AS dist_3200m_restaurant_osm,
dest_distances_3200m.cafe_osm AS dist_3200m_cafe_osm,
dest_distances_3200m.eatery_osm AS dist_3200m_eatery_osm,
dest_distances_3200m.food_court_osm AS dist_3200m_food_court_osm,
dest_distances_3200m.fastfood_osm AS dist_3200m_fastfood_osm,
dest_distances_3200m.pub_osm AS dist_3200m_pub_osm,
dest_distances_3200m.bar_osm AS dist_3200m_bar_osm,
dest_distances_3200m.nightclub_osm AS dist_3200m_nightclub_osm,
dest_distances_3200m.gambling_osm AS dist_3200m_gambling_osm,
dest_distances_3200m.alcohol_osm AS dist_3200m_alcohol_osm,
dest_distances_3200m.tobacco_osm AS dist_3200m_tobacco_osm,
dest_distances_3200m.swimming_pool_osm AS dist_3200m_swimming_pool_osm,
dest_distances_3200m.all_schools AS dist_3200m_all_schools,
dest_distances_3200m.P_12_Schools AS dist_3200m_P_12_Schools,
dest_distances_3200m.primary_schools AS dist_3200m_primary_schools,
dest_distances_3200m.secondary_schools AS dist_3200m_secondary_schools,
dest_distances_3200m.special_schools AS dist_3200m_special_schools,
dest_distances_3200m.libraries AS dist_3200m_libraries,
dest_distances_3200m.gtfs_2018_stop_30_mins_final AS dist_3200m_gtfs_2018_stop_30_mins_final,
dest_distances_3200m.gtfs_2018_stops AS dist_3200m_gtfs_2018_stops,
dest_distances_3200m.gtfs_2018_stops_bus AS dist_3200m_gtfs_2018_stops_bus,
dest_distances_3200m.gtfs_2018_stops_ferry AS dist_3200m_gtfs_2018_stops_ferry,
dest_distances_3200m.gtfs_2018_stops_train AS dist_3200m_gtfs_2018_stops_train,
dest_distances_3200m.gtfs_2018_stops_tram AS dist_3200m_gtfs_2018_stops_tram,
dest_distances_3200m.gtfs_nsw_stop_30_mins_bus AS dist_3200m_gtfs_nsw_stop_30_mins_bus,
dest_distances_3200m.gtfs_nsw_stop_15_mins_train AS dist_3200m_gtfs_nsw_stop_15_mins_train,
dest_distances_3200m.Hospital AS dist_3200m_Hospital,
dest_distances_3200m.PhysicalActivity_Recreation AS dist_3200m_PhysicalActivity_Recreation,
dest_distances_3200m.ChildcareKinder_LongDayChildCare AS dist_3200m_ChildcareKinder_LongDayChildCare,
dest_distances_3200m.ChildcareKinder_Kinder_Preschool AS dist_3200m_ChildcareKinder_Kinder_Preschool,
dest_distances_3200m.ChildcareKinder_HolidayProgram AS dist_3200m_ChildcareKinder_HolidayProgram,
dest_distances_3200m.ChildcareKinder_KinderDisability AS dist_3200m_ChildcareKinder_KinderDisability,
dest_distances_3200m.ChildcareKinder_OSHC AS dist_3200m_ChildcareKinder_OSHC,
dest_distances_3200m.ChildcareKinder_OccasionalCare AS dist_3200m_ChildcareKinder_OccasionalCare,
dest_distances_3200m.ChildcareKinder_FamilyDayCare AS dist_3200m_ChildcareKinder_FamilyDayCare,
dest_distances_3200m.ChildDevelopment_Playgroup AS dist_3200m_ChildDevelopment_Playgroup,
dest_distances_3200m.ChildDevelopment_ParentingFamilySupport AS dist_3200m_ChildDevelopment_ParentingFamilySupport,
dest_distances_3200m.ChildDevelopment_ChildPlayProgram AS dist_3200m_ChildDevelopment_ChildPlayProgram,
dest_distances_3200m.ChildDevelopment_EarlyParentingSupport AS dist_3200m_ChildDevelopment_EarlyParentingSupport,
dest_distances_3200m.ChildDevelopment_ToyLibrary AS dist_3200m_ChildDevelopment_ToyLibrary,
dest_distances_3200m.ChildDevelopment_SchoolNursing AS dist_3200m_ChildDevelopment_SchoolNursing,
dest_distances_3200m.ChildProtectionFamilyServices_Integrated AS dist_3200m_ChildProtectionFamilyServices_Integrated,
dest_distances_3200m.CommunityHealthCare_Pharmacy AS dist_3200m_CommunityHealthCare_Pharmacy,
dest_distances_3200m.CommunityHealthCare_MCH AS dist_3200m_CommunityHealthCare_MCH,
dest_distances_3200m.CommunityHealthCare_Immunisation AS dist_3200m_CommunityHealthCare_Immunisation,
dest_distances_3200m.Counselling_CounsellingFamilyTherapy AS dist_3200m_Counselling_CounsellingFamilyTherapy,
dest_distances_3200m.Counselling_GeneralCounselling AS dist_3200m_Counselling_GeneralCounselling,
dest_distances_3200m.DisabilitySupport_EarlyChildhoodIntervention AS dist_3200m_DisabilitySupport_EarlyChildhoodIntervention,
dest_distances_3200m.EducationLearning_Library AS dist_3200m_EducationLearning_Library,
dest_distances_3200m.GeneralPracticeGP_GP AS dist_3200m_GeneralPracticeGP_GP,
dest_distances_3200m.MentalHealth_ChildMentalHealth AS dist_3200m_MentalHealth_ChildMentalHealth,
dest_distances_3200m.SpecialistPaediatric_PaediatricMedicine AS dist_3200m_SpecialistPaediatric_PaediatricMedicine,
dest_distances_3200m.MentalHealth_GeneralMentalHealthService AS dist_3200m_MentalHealth_GeneralMentalHealthService,
dest_distances_3200m.MentalHealth_AdultMentalHealthService AS dist_3200m_MentalHealth_AdultMentalHealthService,
dest_distances_3200m.MentalHealth_Psychology AS dist_3200m_MentalHealth_Psychology,
dest_distances_3200m.toilets AS dist_3200m_toilets,
dest_distances_3200m.playgrounds AS dist_3200m_playgrounds,
dest_distances_3200m.centrelink AS dist_3200m_centrelink,
-- you could add in new destinations here; be sure to remember to end the line(s) with a comma!!
-- dest_distances_3200m.destination AS dist_3200m_destination,
-- PLEASE UNCOMMENT BELOW FOR ALCOHOL and childcare!
dest_distances_3200m.alcohol_offlicence AS dist_3200m_alcohol_offlicence,
dest_distances_3200m.alcohol_onlicence AS dist_3200m_alcohol_onlicence,
dest_distances_3200m.childcare_all AS dist_3200m_childcare_all,
dest_distances_3200m.childcare_all_meet AS dist_3200m_childcare_all_meet,
dest_distances_3200m.childcare_all_exc AS dist_3200m_childcare_all_exc,
dest_distances_3200m.childcare_oshc AS dist_3200m_childcare_oshc,
dest_distances_3200m.childcare_oshc_meet AS dist_3200m_childcare_oshc_meet,
dest_distances_3200m.childcare_oshc_exc AS dist_3200m_childcare_oshc_exc,
dest_distances_3200m.childcare_preschool AS dist_3200m_childcare_preschool,
dest_distances_3200m.childcare_preschool_meet AS dist_3200m_childcare_preschool_meet,
dest_distances_3200m.childcare_preschool_exc AS dist_3200m_childcare_preschool_exc,
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
LEFT JOIN od_aos_jsonb ON p.gnaf_pid = od_aos_jsonb.gnaf_pid
LEFT JOIN dest_distance_m ON p.gnaf_pid = dest_distance_m.gnaf_pid
LEFT JOIN dest_distances_3200m ON p.gnaf_pid = dest_distances_3200m.gnaf_pid
'''.format(locale=locale)
curs.execute(aedc_measures)
conn.commit()

print("Can you please run the following from the command prompt in the data directory?")
print('''
pg_dump -U postgres -h localhost -W  -t "aedc_measures" -t "open_space_areas" {db} > aedc_{db}.sql
'''.format(locale = locale.lower(), year = year,db = db))

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
