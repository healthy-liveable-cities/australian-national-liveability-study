# Script:  17_aedc_measures.py
# Purpose: Create parcel indicators for national liveability project
# Author:  Carl Higgs 
# Date:    20180717

import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
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
   'SELECT gnaf_pid, lower(dest_name), distance
    FROM   od_closest
    ORDER  BY 1,2'  -- could also just be "ORDER BY 1" here
  ,$$SELECT unnest('{curly_o}{category_list}{curly_c}'::text[])$$
   ) AS distance ("gnaf_pid" text, {category_types});
'''.format(id = points_id.lower(),
           curly_o = "{",
           curly_c = "}",
           category_list = category_list.lower(),
           category_types = category_types.lower())
curs.execute(crosstab)
conn.commit()
print("Done.")

print("Create summary table of destination distance arrays... "),
crosstab = '''
DROP TABLE IF EXISTS dest_distances_3200m;
CREATE TABLE IF NOT EXISTS dest_distances_3200m AS
SELECT *
  FROM   crosstab(
   'SELECT gnaf_pid, lower(dest_class), distances
    FROM   od_distances_3200m
    ORDER  BY 1,2'  -- could also just be "ORDER BY 1" here
  ,$$SELECT unnest('{curly_o}{category_list}{curly_c}'::text[])$$
   ) AS distances ("gnaf_pid" text, {category_types});
'''.format(id = points_id.lower(),
           curly_o = "{",
           curly_c = "}",
           category_list = array_category_list.lower(),
           category_types = array_category_types.lower())
curs.execute(crosstab)
conn.commit()
print("Done.")

print("Ensure dest distance table has all required fields (you may have to edit the script on from line 57!)... "),
dest_distance_m = []
for dest in df_destinations.destination.tolist():
  sql = '''ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS {} int;'''.format(dest)
  curs.execute(sql)
  conn.commit()
  dest_distance_m.append(''' dest_distance_m.{dest} as dist_cl_{dest}, '''.format(dest = dest))
print("Done.")

print("Ensure dest distances 3200 table has all required fields (you may have to edit the script on from line 182!)... "),
dest_distances_3200m = []
for dest_class in df_destinations.destination_class.tolist():
  sql = '''ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS {} int[] ;'''.format(dest_class)
  curs.execute(sql)
  conn.commit()
  dest_distances_3200m.append(' dest_distances_3200m.{dest_class} as dist_3200m_{dest_class}, '.format(dest_class))
print("Done.")

aedc_measures = '''
DROP TABLE IF EXISTS aedc_measures;
CREATE TABLE aedc_measures AS
SELECT
p.gnaf_pid ,
'{locale}' as locale,
p.count_objectid ,
p.point_x ,
p.point_y ,
p.hex_id ,
abs.mb_code_2016 ,
abs.mb_category_name_2016,
abs.dwelling ,
abs.person ,
abs.sa1_maincode ,
abs.sa2_name_2016 ,
abs.sa3_name_2016 ,
abs.sa4_name_2016 ,
abs.gccsa_name ,
abs.state_name ,
non_abs.ssc_code_2016 ,
non_abs.ssc_name_2016 ,
non_abs.lga_code_2016 ,
non_abs.lga_name_2016 ,
sos.sos_name_2016 ,
ind_activity.distance/1000.0 as walk_12,
sc_nh1600m.sc_nh1600m as walk_15,
dd_nh1600m.dd_nh1600m as walk_16,
ind_walkability.wa_soft as walk_17_soft,
ind_walkability.wa_hard as walk_17_hard,
{dest_distance_m}
{dest_distnces_3200m}
dest_distance_m.conveniencestores_2014 as dist_cl_conveniencestores_2014,
dest_distance_m.newsagents_2014 as dist_cl_newsagents_2014,
dest_distance_m.petrolstations_2014 as dist_cl_petrolstations_2014,
dest_distance_m.fastfood_2017 as dist_cl_fastfood_2017,
dest_distance_m.supermarkets_2017 as dist_cl_supermarkets_2017,
dest_distance_m.activity_centres_2017 as dist_cl_activity_centres_2017,
dest_distance_m.supermarket_osm as dist_cl_supermarket_osm,
dest_distance_m.bakery_osm as dist_cl_bakery_osm,
dest_distance_m.meat_seafood_osm as dist_cl_meat_seafood_osm,
dest_distance_m.fruit_veg_osm as dist_cl_fruit_veg_osm,
dest_distance_m.deli_osm as dist_cl_deli_osm,
dest_distance_m.convenience_osm as dist_cl_convenience_osm,
dest_distance_m.petrolstation_osm as dist_cl_petrolstation_osm,
dest_distance_m.newsagent_osm as dist_cl_newsagent_osm,
dest_distance_m.food_other_osm as dist_cl_food_other_osm,
dest_distance_m.food_health_osm as dist_cl_food_health_osm,
dest_distance_m.market_osm as dist_cl_market_osm,
dest_distance_m.community_centre_osm as dist_cl_community_centre_osm,
dest_distance_m.place_of_worship_osm as dist_cl_place_of_worship_osm,
dest_distance_m.museum_osm as dist_cl_museum_osm,
dest_distance_m.theatre_osm as dist_cl_theatre_osm,
dest_distance_m.cinema_osm as dist_cl_cinema_osm,
dest_distance_m.art_gallery_osm as dist_cl_art_gallery_osm,
dest_distance_m.art_centre_osm as dist_cl_art_centre_osm,
dest_distance_m.artwork_osm as dist_cl_artwork_osm,
dest_distance_m.fountain_osm as dist_cl_fountain_osm,
dest_distance_m.viewpoint_osm as dist_cl_viewpoint_osm,
dest_distance_m.picnic_site_osm as dist_cl_picnic_site_osm,
dest_distance_m.pharmacy_osm as dist_cl_pharmacy_osm,
dest_distance_m.restaurant_osm as dist_cl_restaurant_osm,
dest_distance_m.cafe_osm as dist_cl_cafe_osm,
dest_distance_m.eatery_osm as dist_cl_eatery_osm,
dest_distance_m.food_court_osm as dist_cl_food_court_osm,
dest_distance_m.fastfood_osm as dist_cl_fastfood_osm,
dest_distance_m.pub_osm as dist_cl_pub_osm,
dest_distance_m.bar_osm as dist_cl_bar_osm,
dest_distance_m.nightclub_osm as dist_cl_nightclub_osm,
dest_distance_m.gambling_osm as dist_cl_gambling_osm,
dest_distance_m.alcohol_osm as dist_cl_alcohol_osm,
dest_distance_m.tobacco_osm as dist_cl_tobacco_osm,
dest_distance_m.swimming_pool_osm as dist_cl_swimming_pool_osm,
dest_distance_m.all_schools2018 as dist_cl_all_schools2018,
dest_distance_m.p_12_schools2018 as dist_cl_p_12_schools2018,
dest_distance_m.primary_schools2018 as dist_cl_primary_schools2018,
dest_distance_m.secondary_schools2018 as dist_cl_secondary_schools2018,
dest_distance_m.special_schools2018 as dist_cl_special_schools2018,
dest_distance_m.libraries_2018 as dist_cl_libraries_2018,
dest_distance_m.gtfs_2018_stop_30_mins_final as dist_cl_gtfs_2018_stop_30_mins_final,
dest_distance_m.gtfs_2018_stops as dist_cl_gtfs_2018_stops,
dest_distance_m.gtfs_2018_stops_bus as dist_cl_gtfs_2018_stops_bus,
dest_distance_m.gtfs_2018_stops_ferry as dist_cl_gtfs_2018_stops_ferry,
dest_distance_m.gtfs_2018_stops_train as dist_cl_gtfs_2018_stops_train,
dest_distance_m.gtfs_2018_stops_tram as dist_cl_gtfs_2018_stops_tram,
dest_distance_m.gtfs_nsw_stop_30_mins_bus as dist_cl_gtfs_nsw_stop_30_mins_bus,
dest_distance_m.gtfs_nsw_stop_15_mins_train as dist_cl_gtfs_nsw_stop_15_mins_train,
dest_distance_m.hospital as dist_cl_hospital,
dest_distance_m.physicalactivity_recreation as dist_cl_physicalactivity_recreation,
dest_distance_m.childcarekinder_longdaychildcare as dist_cl_childcarekinder_longdaychildcare,
dest_distance_m.childcarekinder_kinder_preschool as dist_cl_childcarekinder_kinder_preschool,
dest_distance_m.childcarekinder_holidayprogram as dist_cl_childcarekinder_holidayprogram,
dest_distance_m.childcarekinder_kinderdisability as dist_cl_childcarekinder_kinderdisability,
dest_distance_m.childcarekinder_oshc as dist_cl_childcarekinder_oshc,
dest_distance_m.childcarekinder_occasionalcare as dist_cl_childcarekinder_occasionalcare,
dest_distance_m.childcarekinder_familydaycare as dist_cl_childcarekinder_familydaycare,
dest_distance_m.childdevelopment_playgroup as dist_cl_childdevelopment_playgroup,
dest_distance_m.childdevelopment_parentingfamilysupport as dist_cl_childdevelopment_parentingfamilysupport,
dest_distance_m.childdevelopment_childplayprogram as dist_cl_childdevelopment_childplayprogram,
dest_distance_m.childdevelopment_earlyparentingsupport as dist_cl_childdevelopment_earlyparentingsupport,
dest_distance_m.childdevelopment_toylibrary as dist_cl_childdevelopment_toylibrary,
dest_distance_m.childdevelopment_schoolnursing as dist_cl_childdevelopment_schoolnursing,
dest_distance_m.childprotectionfamilyservices_integrated as dist_cl_childprotectionfamilyservices_integrated,
dest_distance_m.communityhealthcare_pharmacy as dist_cl_communityhealthcare_pharmacy,
dest_distance_m.communityhealthcare_mch as dist_cl_communityhealthcare_mch,
dest_distance_m.communityhealthcare_immunisation as dist_cl_communityhealthcare_immunisation,
dest_distance_m.counselling_counsellingfamilytherapy as dist_cl_counselling_counsellingfamilytherapy,
dest_distance_m.counselling_generalcounselling as dist_cl_counselling_generalcounselling,
dest_distance_m.disabilitysupport_earlychildhoodintervention as dist_cl_disabilitysupport_earlychildhoodintervention,
dest_distance_m.educationlearning_library as dist_cl_educationlearning_library,
dest_distance_m.generalpracticegp_gp as dist_cl_generalpracticegp_gp,
dest_distance_m.mentalhealth_childmentalhealth as dist_cl_mentalhealth_childmentalhealth,
dest_distance_m.specialistpaediatric_paediatricmedicine as dist_cl_specialistpaediatric_paediatricmedicine,
dest_distance_m.mentalhealth_generalmentalhealthservice as dist_cl_mentalhealth_generalmentalhealthservice,
dest_distance_m.mentalhealth_adultmentalhealthservice as dist_cl_mentalhealth_adultmentalhealthservice,
dest_distance_m.mentalhealth_psychology as dist_cl_mentalhealth_psychology,
dest_distance_m.playgrounds_2018 as dist_cl_playgrounds_2018,
dest_distance_m.centrelink_2018 as dist_cl_centrelink_2018,
-- you could add in new destinations here; be sure to remember to end the line(s) with a comma!! eg.
-- dest_distance_m.destination as dist_cl_destination,
-- please uncomment below for alchol!!
dest_distance_m.alcohol_offlicence as dist_cl_alcohol_offlicence,
dest_distance_m.alcohol_onlicence as dist_cl_alcohol_onlicence,
dest_distance_m.childcare_2018 as dist_cl_childcare_2018,
dest_distance_m.childcare_meet_2018 as dist_cl_childcare_meet_2018,
dest_distance_m.childcare_exc_2018 as dist_cl_childcare_exc_2018,
dest_distance_m.childcare_oshc_2018 as dist_cl_childcare_oshc_2018,
dest_distance_m.childcare_oshc_meet_2018 as dist_cl_childcare_oshc_meet_2018,
dest_distance_m.childcare_oshc_exc_2018 as dist_cl_childcare_oshc_exc_2018,
dest_distance_m.preschool_childcare_2018 as dist_cl_preschool_childcare_2018,
dest_distance_m.preschool_childcare_meet_2018 as dist_cl_preschool_childcare_meet_2018,
dest_distance_m.preschool_childcare_exc_2018 as dist_cl_preschool_childcare_exc_2018,
dest_distances_3200m.convenience as dist_3200m_convenience,
dest_distances_3200m.newsagent as dist_3200m_newsagent,
dest_distances_3200m.petrolstation as dist_3200m_petrolstation,
dest_distances_3200m.fast_food as dist_3200m_fast_food,
dest_distances_3200m.supermarket as dist_3200m_supermarket,
dest_distances_3200m.activity_centres as dist_3200m_activity_centres,
dest_distances_3200m.supermarket_osm as dist_3200m_supermarket_osm,
dest_distances_3200m.bakery_osm as dist_3200m_bakery_osm,
dest_distances_3200m.meat_seafood_osm as dist_3200m_meat_seafood_osm,
dest_distances_3200m.fruit_veg_osm as dist_3200m_fruit_veg_osm,
dest_distances_3200m.deli_osm as dist_3200m_deli_osm,
dest_distances_3200m.convenience_osm as dist_3200m_convenience_osm,
dest_distances_3200m.petrolstation_osm as dist_3200m_petrolstation_osm,
dest_distances_3200m.newsagent_osm as dist_3200m_newsagent_osm,
dest_distances_3200m.food_other_osm as dist_3200m_food_other_osm,
dest_distances_3200m.food_health_osm as dist_3200m_food_health_osm,
dest_distances_3200m.market_osm as dist_3200m_market_osm,
dest_distances_3200m.community_centre_osm as dist_3200m_community_centre_osm,
dest_distances_3200m.place_of_worship_osm as dist_3200m_place_of_worship_osm,
dest_distances_3200m.museum_osm as dist_3200m_museum_osm,
dest_distances_3200m.theatre_osm as dist_3200m_theatre_osm,
dest_distances_3200m.cinema_osm as dist_3200m_cinema_osm,
dest_distances_3200m.art_gallery_osm as dist_3200m_art_gallery_osm,
dest_distances_3200m.art_centre_osm as dist_3200m_art_centre_osm,
dest_distances_3200m.artwork_osm as dist_3200m_artwork_osm,
dest_distances_3200m.fountain_osm as dist_3200m_fountain_osm,
dest_distances_3200m.viewpoint_osm as dist_3200m_viewpoint_osm,
dest_distances_3200m.picnic_site_osm as dist_3200m_picnic_site_osm,
dest_distances_3200m.pharmacy_osm as dist_3200m_pharmacy_osm,
dest_distances_3200m.restaurant_osm as dist_3200m_restaurant_osm,
dest_distances_3200m.cafe_osm as dist_3200m_cafe_osm,
dest_distances_3200m.eatery_osm as dist_3200m_eatery_osm,
dest_distances_3200m.food_court_osm as dist_3200m_food_court_osm,
dest_distances_3200m.fastfood_osm as dist_3200m_fastfood_osm,
dest_distances_3200m.pub_osm as dist_3200m_pub_osm,
dest_distances_3200m.bar_osm as dist_3200m_bar_osm,
dest_distances_3200m.nightclub_osm as dist_3200m_nightclub_osm,
dest_distances_3200m.gambling_osm as dist_3200m_gambling_osm,
dest_distances_3200m.alcohol_osm as dist_3200m_alcohol_osm,
dest_distances_3200m.tobacco_osm as dist_3200m_tobacco_osm,
dest_distances_3200m.swimming_pool_osm as dist_3200m_swimming_pool_osm,
dest_distances_3200m.all_schools as dist_3200m_all_schools,
dest_distances_3200m.p_12_schools as dist_3200m_p_12_schools,
dest_distances_3200m.primary_schools as dist_3200m_primary_schools,
dest_distances_3200m.secondary_schools as dist_3200m_secondary_schools,
dest_distances_3200m.special_schools as dist_3200m_special_schools,
dest_distances_3200m.libraries as dist_3200m_libraries,
dest_distances_3200m.gtfs_2018_stop_30_mins_final as dist_3200m_gtfs_2018_stop_30_mins_final,
dest_distances_3200m.gtfs_2018_stops as dist_3200m_gtfs_2018_stops,
dest_distances_3200m.gtfs_2018_stops_bus as dist_3200m_gtfs_2018_stops_bus,
dest_distances_3200m.gtfs_2018_stops_ferry as dist_3200m_gtfs_2018_stops_ferry,
dest_distances_3200m.gtfs_2018_stops_train as dist_3200m_gtfs_2018_stops_train,
dest_distances_3200m.gtfs_2018_stops_tram as dist_3200m_gtfs_2018_stops_tram,
dest_distances_3200m.gtfs_nsw_stop_30_mins_bus as dist_3200m_gtfs_nsw_stop_30_mins_bus,
dest_distances_3200m.gtfs_nsw_stop_15_mins_train as dist_3200m_gtfs_nsw_stop_15_mins_train,
dest_distances_3200m.hospital as dist_3200m_hospital,
dest_distances_3200m.physicalactivity_recreation as dist_3200m_physicalactivity_recreation,
dest_distances_3200m.childcarekinder_longdaychildcare as dist_3200m_childcarekinder_longdaychildcare,
dest_distances_3200m.childcarekinder_kinder_preschool as dist_3200m_childcarekinder_kinder_preschool,
dest_distances_3200m.childcarekinder_holidayprogram as dist_3200m_childcarekinder_holidayprogram,
dest_distances_3200m.childcarekinder_kinderdisability as dist_3200m_childcarekinder_kinderdisability,
dest_distances_3200m.childcarekinder_oshc as dist_3200m_childcarekinder_oshc,
dest_distances_3200m.childcarekinder_occasionalcare as dist_3200m_childcarekinder_occasionalcare,
dest_distances_3200m.childcarekinder_familydaycare as dist_3200m_childcarekinder_familydaycare,
dest_distances_3200m.childdevelopment_playgroup as dist_3200m_childdevelopment_playgroup,
dest_distances_3200m.childdevelopment_parentingfamilysupport as dist_3200m_childdevelopment_parentingfamilysupport,
dest_distances_3200m.childdevelopment_childplayprogram as dist_3200m_childdevelopment_childplayprogram,
dest_distances_3200m.childdevelopment_earlyparentingsupport as dist_3200m_childdevelopment_earlyparentingsupport,
dest_distances_3200m.childdevelopment_toylibrary as dist_3200m_childdevelopment_toylibrary,
dest_distances_3200m.childdevelopment_schoolnursing as dist_3200m_childdevelopment_schoolnursing,
dest_distances_3200m.childprotectionfamilyservices_integrated as dist_3200m_childprotectionfamilyservices_integrated,
dest_distances_3200m.communityhealthcare_pharmacy as dist_3200m_communityhealthcare_pharmacy,
dest_distances_3200m.communityhealthcare_mch as dist_3200m_communityhealthcare_mch,
dest_distances_3200m.communityhealthcare_immunisation as dist_3200m_communityhealthcare_immunisation,
dest_distances_3200m.counselling_counsellingfamilytherapy as dist_3200m_counselling_counsellingfamilytherapy,
dest_distances_3200m.counselling_generalcounselling as dist_3200m_counselling_generalcounselling,
dest_distances_3200m.disabilitysupport_earlychildhoodintervention as dist_3200m_disabilitysupport_earlychildhoodintervention,
dest_distances_3200m.educationlearning_library as dist_3200m_educationlearning_library,
dest_distances_3200m.generalpracticegp_gp as dist_3200m_generalpracticegp_gp,
dest_distances_3200m.mentalhealth_childmentalhealth as dist_3200m_mentalhealth_childmentalhealth,
dest_distances_3200m.specialistpaediatric_paediatricmedicine as dist_3200m_specialistpaediatric_paediatricmedicine,
dest_distances_3200m.mentalhealth_generalmentalhealthservice as dist_3200m_mentalhealth_generalmentalhealthservice,
dest_distances_3200m.mentalhealth_adultmentalhealthservice as dist_3200m_mentalhealth_adultmentalhealthservice,
dest_distances_3200m.mentalhealth_psychology as dist_3200m_mentalhealth_psychology,
dest_distances_3200m.playgrounds as dist_3200m_playgrounds,
dest_distances_3200m.centrelink as dist_3200m_centrelink,
-- you could add in new destinations here; be sure to remember to end the line(s) with a comma!!
-- dest_distances_3200m.destination as dist_3200m_destination,
-- please uncomment below for alcohol and childcare!
dest_distances_3200m.alcohol_offlicence as dist_3200m_alcohol_offlicence,
dest_distances_3200m.alcohol_onlicence as dist_3200m_alcohol_onlicence,
dest_distances_3200m.childcare_all as dist_3200m_childcare_all,
dest_distances_3200m.childcare_all_meet as dist_3200m_childcare_all_meet,
dest_distances_3200m.childcare_all_exc as dist_3200m_childcare_all_exc,
dest_distances_3200m.childcare_oshc as dist_3200m_childcare_oshc,
dest_distances_3200m.childcare_oshc_meet as dist_3200m_childcare_oshc_meet,
dest_distances_3200m.childcare_oshc_exc as dist_3200m_childcare_oshc_exc,
dest_distances_3200m.childcare_preschool as dist_3200m_childcare_preschool,
dest_distances_3200m.childcare_preschool_meet as dist_3200m_childcare_preschool_meet,
dest_distances_3200m.childcare_preschool_exc as dist_3200m_childcare_preschool_exc,
od_aos_jsonb.attributes as aos_distances,
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
LEFT JOIN dest_distances_3200m ON p.gnaf_pid = dest_distances_3200m.gnaf_pid;
CREATE UNIQUE INDEX aedc__measures_idx ON aedc_measures (gnaf_pid);  
'''.format(locale=locale)
curs.execute(aedc_measures)
conn.commit()

print('''Analyse the AEDC measures table... '''),
curs.execute('''ANALYZE aedc_measures;''')
conn.commit()
print("Done.")

print('''
Prepare report table (aedc_null_fraction) on proportion of rows that are null.  That is,
  - if null_fract is 1 for a variable, then 100% are null.  Please check:
      - perhaps no destinations of this type in your region?
      - or some processing stage has been missed?
  - if null_fract is .01 for a variable, then 1% are null  (which is still quite large and worth investigating)
  - if null_fract is .0001 for a variable, then 1 in 10000 are null which may be realistic
''')
null_check = '''
DROP TABLE IF EXISTS aedc_null_fraction;
CREATE TABLE aedc_null_fraction AS
SELECT locale.locale, 
       attname,
       null_frac 
FROM pg_stats,
     (SELECT locale::text FROM aedc_measures LIMIT 1) locale 
WHERE pg_stats."tablename" = 'aedc_measures';
'''
curs.execute(null_check)
conn.commit()
print("Done.")

print('''Add locale column to open_space_areas in preparation for merge with other data... '''),
aos_locale = '''
ALTER TABLE open_space_areas ADD COLUMN IF NOT EXISTS locale text;
UPDATE open_space_areas SET locale = '{}' ;
'''.format(locale.lower())
curs.execute(aos_locale)
conn.commit()
print("Done.")

print("Exporting aedc measures, null fraction check, and open space areas to study region's data directory..."),
# command = '''
# pg_dump -U postgres -h localhost -W  -t "aedc_measures" -t "aedc_null_fraction" -t "open_space_areas" {db} > aedc_{db}.sql
# '''.format(locale = locale.lower(), year = year,db = db)
# sp.call(command, shell=True, cwd=folderPath)                           
for table in ['aedc_measures','aedc_null_fraction','open_space_areas']:
  file = os.path.join(locale_dir,'{db}_{table}.csv'.format(db = db,table = table))
  with open(file, 'w') as f:
    sql = '''COPY {table} TO STDOUT WITH DELIMITER ';' CSV HEADER;'''.format(table = table)
    curs.copy_expert(sql,f)
  
print("Done.")

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
