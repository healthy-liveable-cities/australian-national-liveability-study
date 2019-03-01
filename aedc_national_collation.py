# Script:  aedc_national_collation.py
# Purpose: Collate study region csv output for aedc measures
# Author:  Carl Higgs 
# Date:    20180717
# Note:    Assumes the li_australia_2018 database has been created.  
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
db = 'li_australia_2018'
print("This script assumes the database {db} has been created!".format(db = db))
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

create_tables = '''
DROP TABLE IF EXISTS aedc_measures;
create table aedc_measures
(
	gnaf_pid varchar(15),
	locale text,
	count_objectid integer,
	point_x double precision,
	point_y double precision,
	hex_id integer,
	mb_code_2016 text,
	mb_category_name_2016 text,
	dwelling double precision,
	person bigint,
	sa1_maincode varchar(11),
	sa2_name_2016 varchar(50),
	sa3_name_2016 varchar(50),
	sa4_name_2016 varchar(50),
	gccsa_name varchar(50),
	state_name varchar(50),
	ssc_code_2016 varchar,
	ssc_name_2016 varchar,
	lga_code_2016 varchar,
	lga_name_2016 varchar,
	sos_name_2016 varchar,
	walk_12 numeric,
	walk_15 double precision,
	walk_16 double precision,
	walk_17_soft double precision,
	walk_17_hard double precision,
	dist_cl_conveniencestores_2014 integer,
	dist_cl_newsagents_2014 integer,
	dist_cl_petrolstations_2014 integer,
	dist_cl_fastfood_2017 integer,
	dist_cl_supermarkets_2017 integer,
	dist_cl_activity_centres_2017 integer,
	dist_cl_supermarket_osm integer,
	dist_cl_bakery_osm integer,
	dist_cl_meat_seafood_osm integer,
	dist_cl_fruit_veg_osm integer,
	dist_cl_deli_osm integer,
	dist_cl_convenience_osm integer,
	dist_cl_petrolstation_osm integer,
	dist_cl_newsagent_osm integer,
	dist_cl_food_other_osm integer,
	dist_cl_food_health_osm integer,
	dist_cl_market_osm integer,
	dist_cl_community_centre_osm integer,
	dist_cl_place_of_worship_osm integer,
	dist_cl_museum_osm integer,
	dist_cl_theatre_osm integer,
	dist_cl_cinema_osm integer,
	dist_cl_art_gallery_osm integer,
	dist_cl_art_centre_osm integer,
	dist_cl_artwork_osm integer,
	dist_cl_fountain_osm integer,
	dist_cl_viewpoint_osm integer,
	dist_cl_picnic_site_osm integer,
	dist_cl_pharmacy_osm integer,
	dist_cl_restaurant_osm integer,
	dist_cl_cafe_osm integer,
	dist_cl_eatery_osm integer,
	dist_cl_food_court_osm integer,
	dist_cl_fastfood_osm integer,
	dist_cl_pub_osm integer,
	dist_cl_bar_osm integer,
	dist_cl_nightclub_osm integer,
	dist_cl_gambling_osm integer,
	dist_cl_alcohol_osm integer,
	dist_cl_tobacco_osm integer,
	dist_cl_swimming_pool_osm integer,
	dist_cl_all_schools2018 integer,
	dist_cl_p_12_schools2018 integer,
	dist_cl_primary_schools2018 integer,
	dist_cl_secondary_schools2018 integer,
	dist_cl_special_schools2018 integer,
	dist_cl_libraries_2018 integer,
	dist_cl_gtfs_2018_stop_30_mins_final integer,
	dist_cl_gtfs_2018_stops integer,
	dist_cl_gtfs_2018_stops_bus integer,
	dist_cl_gtfs_2018_stops_ferry integer,
	dist_cl_gtfs_2018_stops_train integer,
	dist_cl_gtfs_2018_stops_tram integer,
	dist_cl_gtfs_nsw_stop_30_mins_bus integer,
	dist_cl_gtfs_nsw_stop_15_mins_train integer,
	dist_cl_hospital integer,
	dist_cl_physicalactivity_recreation integer,
	dist_cl_childcarekinder_longdaychildcare integer,
	dist_cl_childcarekinder_kinder_preschool integer,
	dist_cl_childcarekinder_holidayprogram integer,
	dist_cl_childcarekinder_kinderdisability integer,
	dist_cl_childcarekinder_oshc integer,
	dist_cl_childcarekinder_occasionalcare integer,
	dist_cl_childcarekinder_familydaycare integer,
	dist_cl_childdevelopment_playgroup integer,
	dist_cl_childdevelopment_parentingfamilysupport integer,
	dist_cl_childdevelopment_childplayprogram integer,
	dist_cl_childdevelopment_earlyparentingsupport integer,
	dist_cl_childdevelopment_toylibrary integer,
	dist_cl_childdevelopment_schoolnursing integer,
	dist_cl_childprotectionfamilyservices_integrated integer,
	dist_cl_communityhealthcare_pharmacy integer,
	dist_cl_communityhealthcare_mch integer,
	dist_cl_communityhealthcare_immunisation integer,
	dist_cl_counselling_counsellingfamilytherapy integer,
	dist_cl_counselling_generalcounselling integer,
	dist_cl_disabilitysupport_earlychildhoodintervention integer,
	dist_cl_educationlearning_library integer,
	dist_cl_generalpracticegp_gp integer,
	dist_cl_mentalhealth_childmentalhealth integer,
	dist_cl_specialistpaediatric_paediatricmedicine integer,
	dist_cl_mentalhealth_generalmentalhealthservice integer,
	dist_cl_mentalhealth_adultmentalhealthservice integer,
	dist_cl_mentalhealth_psychology integer,
	dist_cl_playgrounds_2018 integer,
	dist_cl_centrelink_2018 integer,
	dist_cl_alcohol_offlicence integer,
	dist_cl_alcohol_onlicence integer,
	dist_cl_childcare_2018 integer,
	dist_cl_childcare_meet_2018 integer,
	dist_cl_childcare_exc_2018 integer,
	dist_cl_childcare_oshc_2018 integer,
	dist_cl_childcare_oshc_meet_2018 integer,
	dist_cl_childcare_oshc_exc_2018 integer,
	dist_cl_preschool_childcare_2018 integer,
	dist_cl_preschool_childcare_meet_2018 integer,
	dist_cl_preschool_childcare_exc_2018 integer,
	dist_3200m_convenience integer[],
	dist_3200m_newsagent integer[],
	dist_3200m_petrolstation integer[],
	dist_3200m_fast_food integer[],
	dist_3200m_supermarket integer[],
	dist_3200m_activity_centres integer[],
	dist_3200m_supermarket_osm integer[],
	dist_3200m_bakery_osm integer[],
	dist_3200m_meat_seafood_osm integer[],
	dist_3200m_fruit_veg_osm integer[],
	dist_3200m_deli_osm integer[],
	dist_3200m_convenience_osm integer[],
	dist_3200m_petrolstation_osm integer[],
	dist_3200m_newsagent_osm integer[],
	dist_3200m_food_other_osm integer[],
	dist_3200m_food_health_osm integer[],
	dist_3200m_market_osm integer[],
	dist_3200m_community_centre_osm integer[],
	dist_3200m_place_of_worship_osm integer[],
	dist_3200m_museum_osm integer[],
	dist_3200m_theatre_osm integer[],
	dist_3200m_cinema_osm integer[],
	dist_3200m_art_gallery_osm integer[],
	dist_3200m_art_centre_osm integer[],
	dist_3200m_artwork_osm integer[],
	dist_3200m_fountain_osm integer[],
	dist_3200m_viewpoint_osm integer[],
	dist_3200m_picnic_site_osm integer[],
	dist_3200m_pharmacy_osm integer[],
	dist_3200m_restaurant_osm integer[],
	dist_3200m_cafe_osm integer[],
	dist_3200m_eatery_osm integer[],
	dist_3200m_food_court_osm integer[],
	dist_3200m_fastfood_osm integer[],
	dist_3200m_pub_osm integer[],
	dist_3200m_bar_osm integer[],
	dist_3200m_nightclub_osm integer[],
	dist_3200m_gambling_osm integer[],
	dist_3200m_alcohol_osm integer[],
	dist_3200m_tobacco_osm integer[],
	dist_3200m_swimming_pool_osm integer[],
	dist_3200m_all_schools integer[],
	dist_3200m_p_12_schools integer[],
	dist_3200m_primary_schools integer[],
	dist_3200m_secondary_schools integer[],
	dist_3200m_special_schools integer[],
	dist_3200m_libraries integer[],
	dist_3200m_gtfs_2018_stop_30_mins_final integer[],
	dist_3200m_gtfs_2018_stops integer[],
	dist_3200m_gtfs_2018_stops_bus integer[],
	dist_3200m_gtfs_2018_stops_ferry integer[],
	dist_3200m_gtfs_2018_stops_train integer[],
	dist_3200m_gtfs_2018_stops_tram integer[],
	dist_3200m_gtfs_nsw_stop_30_mins_bus integer[],
	dist_3200m_gtfs_nsw_stop_15_mins_train integer[],
	dist_3200m_hospital integer[],
	dist_3200m_physicalactivity_recreation integer[],
	dist_3200m_childcarekinder_longdaychildcare integer[],
	dist_3200m_childcarekinder_kinder_preschool integer[],
	dist_3200m_childcarekinder_holidayprogram integer[],
	dist_3200m_childcarekinder_kinderdisability integer[],
	dist_3200m_childcarekinder_oshc integer[],
	dist_3200m_childcarekinder_occasionalcare integer[],
	dist_3200m_childcarekinder_familydaycare integer[],
	dist_3200m_childdevelopment_playgroup integer[],
	dist_3200m_childdevelopment_parentingfamilysupport integer[],
	dist_3200m_childdevelopment_childplayprogram integer[],
	dist_3200m_childdevelopment_earlyparentingsupport integer[],
	dist_3200m_childdevelopment_toylibrary integer[],
	dist_3200m_childdevelopment_schoolnursing integer[],
	dist_3200m_childprotectionfamilyservices_integrated integer[],
	dist_3200m_communityhealthcare_pharmacy integer[],
	dist_3200m_communityhealthcare_mch integer[],
	dist_3200m_communityhealthcare_immunisation integer[],
	dist_3200m_counselling_counsellingfamilytherapy integer[],
	dist_3200m_counselling_generalcounselling integer[],
	dist_3200m_disabilitysupport_earlychildhoodintervention integer[],
	dist_3200m_educationlearning_library integer[],
	dist_3200m_generalpracticegp_gp integer[],
	dist_3200m_mentalhealth_childmentalhealth integer[],
	dist_3200m_specialistpaediatric_paediatricmedicine integer[],
	dist_3200m_mentalhealth_generalmentalhealthservice integer[],
	dist_3200m_mentalhealth_adultmentalhealthservice integer[],
	dist_3200m_mentalhealth_psychology integer[],
	dist_3200m_playgrounds integer[],
	dist_3200m_centrelink integer[],
	dist_3200m_alcohol_offlicence integer[],
	dist_3200m_alcohol_onlicence integer[],
	dist_3200m_childcare_all integer[],
	dist_3200m_childcare_all_meet integer[],
	dist_3200m_childcare_all_exc integer[],
	dist_3200m_childcare_oshc integer[],
	dist_3200m_childcare_oshc_meet integer[],
	dist_3200m_childcare_oshc_exc integer[],
	dist_3200m_childcare_preschool integer[],
	dist_3200m_childcare_preschool_meet integer[],
	dist_3200m_childcare_preschool_exc integer[],
	aos_distances jsonb,
	geom geometry(Point,7845)
);

-- alter table aedc_measures owner to python;

create unique index aedc_measures_idx
	on aedc_measures (gnaf_pid);

DROP TABLE IF EXISTS aedc_null_fraction;
create table aedc_null_fraction
(
	locale unknown,
	attname name,
	null_frac real
);

--alter table aedc_null_fraction owner to python;

DROP TABLE IF EXISTS  open_space_areas;
create table open_space_areas
(
	aos_id bigint,
	attributes jsonb,
	numgeom bigint,
	geom_public geometry,
	geom_not_public geometry,
	geom_water geometry,
	geom geometry,
	aos_ha_public double precision,
	aos_ha_not_public double precision,
	aos_ha double precision,
	aos_ha_water double precision,
	has_water_feature boolean,
	water_percent numeric,
	co_location_100m jsonb,
	locale text
);

--alter table open_space_areas owner to python;

create unique index aos_idx
	on open_space_areas (locale,aos_id);

CREATE INDEX idx_aos_jsb ON open_space_areas USING GIN (attributes);
'''
curs.execute(create_tables)
conn.commit()

print("Looping over study regions to check if required output exists in their folder; if it does, its imported...")
for locale in study_regions:
  print(" - {locale}".format(locale = locale))
  for table in ['aedc_measures', 'aedc_null_fraction', 'open_space_areas']:
    file = 'D:/ntnl_li_2018_template/data/study_region/{locale}/li_{locale}_2018_{table}.csv'.format(locale = locale,table = table)
    if os.path.isfile(file):
      print("    - {} ".format(table))
      with open(file, 'r') as f:
        sql = '''COPY {table} FROM STDIN WITH DELIMITER ';' CSV HEADER;'''.format(table = table)
        curs.copy_expert(sql,f)
        conn.commit()
         
conn.close()



