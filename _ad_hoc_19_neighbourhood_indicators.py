# Script:  _ad_hoc_19_neighbourhood_indicators.py
# Purpose: Compile destinations results and neighbourhood indicator tables
# Author:  Carl Higgs 
# Date:    20191204


# NOTE original script 19 has other code which is still required and has not been refactored into this script

import os
import sys
import time
import psycopg2 

from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables (November 2019 partial update)'

print('''
This script will create a number of destination indicator tables, 
which can later be drawn on in other scripts, or used as final 
outputs.

But, please note: numerous clauses to only create tables if they 
do not already exist have been added.  If tables are wanted to be 
modified or recreated, some additional tweaking in script or 
interactively (eg to manually drop the table) will be required.

All good? Great - go!
''')

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

print("Create area level destination counts... ")
# We drop these tables first, since some destinations may have been processed since previously running.
# These queries are quick to run, so not much cost to drop and create again.
for area in analysis_regions:
  area_id = df_regions.loc[area,'id']
  abbrev = df_regions.loc[area,'abbreviation']
  print("{}... ".format(area)),
  query = '''
  -- DROP TABLE IF EXISTS {abbrev}_dest_counts;
  CREATE TABLE IF NOT EXISTS {abbrev}_dest_counts AS
  SELECT a.{area_id}, dest_class, count(d.geom) AS count
  FROM area_linkage a
  LEFT JOIN 
       study_destinations d ON st_contains(a.geom,d.geom)
  GROUP BY a.{area_id},dest_class
  ORDER BY a.{area_id},dest_class;  
  '''.format(abbrev = abbrev,
             area_table = 'area_{}_included'.format(abbrev),
             area_id = area_id)
  # print(query)
  curs.execute(query)
  conn.commit()
  print("Done.")

print('Creating or replacing threshold functions ... '),
create_threshold_functions = '''
-- Function for returning counts of values in an array less than a threshold distance
-- e.g. an array of distances in m to destinations, evaluated against a threshold of 800m
-- SELECT gnaf_pid, count_in_threshold(distances,1600) FROM sport_3200m;
-- is equivalent to 
-- SELECT gnaf_pid, count(distances) 
--   FROM (SELECT gnaf_pid,unnest(array_agg) distances FROM sport_3200m) t 
-- WHERE distance < 1600 GROUP BY gnaf_pid;
CREATE OR REPLACE FUNCTION count_in_threshold(distances int[],threshold int) returns bigint as $$
    SELECT COUNT(*) 
    FROM unnest(distances) dt(b)
    WHERE b < threshold
$$ language sql;

-- return minimum value of an integer array (specifically here, used for distance to closest within 3200m)
CREATE OR REPLACE FUNCTION array_min(integers int[]) returns int as $$
    SELECT min(integers) 
    FROM unnest(integers) integers
$$ language sql;

-- append value to array if > some threshold (default, 3200)
CREATE OR REPLACE FUNCTION array_append_if_gr(distances int[],distance int,threshold int default 3200) returns int[] as $$
BEGIN
-- function to append an integer to an array of integers if it is larger than some given threshold 
-- (ie. add in distance to closest to 3200m distances array if the distance to closest value is > 3200m
-- Example applied usage:
-- SELECT gnaf_pid, 
        -- array_append_if_gr(dests.alcohol_offlicence,cl.alcohol_offlicence) AS array,
        -- cl.alcohol_offlicence AS distance
-- FROM dest_distances_3200m dests 
-- LEFT JOIN dest_distance_m cl
-- USING (gnaf_pid) 
-- WHERE cl.alcohol_offlicence > 3200;
IF ((distance <= threshold) OR (distance IS NULL)) 
    THEN RETURN distances;
ELSE 
    RETURN array_append(distances,distance);
END IF;
END;
$$
LANGUAGE plpgsql;  

-- a binary threshold indicator  (e.g. of access given distance and threshold)
CREATE OR REPLACE FUNCTION threshold_hard(distance int, threshold int, out int) 
    RETURNS NULL ON NULL INPUT
    AS $$ SELECT (distance < threshold)::int $$
    LANGUAGE SQL;

-- a soft threshold indicator (e.g. of access given distance and threshold)
CREATE OR REPLACE FUNCTION threshold_soft(distance int, threshold int) returns float AS 
$$
BEGIN
  -- We check to see if the value we are exponentiation is more or less than 100; if so,
  -- if so the result will be more or less either 1 or 0, respectively. 
  -- If the value we are exponentiating is much > abs(700) then we risk overflow/underflow error
  -- due to the value exceeding the numerical limits of postgresql
  -- If the value we are exponentiating is based on a positive distance, then we know it is invalid!
  -- For reference, a 10km distance with 400m threshold yields a check value of -120, 
  -- the exponent of which is 1.30418087839363e+052 and 1 - 1/(1+exp(-120)) is basically 1 - 1 = 0
  -- Using a check value of -100, the point at which zero is returned with a threshold of 400 
  -- is for distance of 3339km
  IF (distance < 0) 
      THEN RETURN NULL;
  ELSIF (-5*(distance-threshold)/(threshold::float) < -100) 
    THEN RETURN 0;
  ELSE 
    RETURN 1 - 1/(1+exp(-5*(distance-threshold)/(threshold::float)));
  END IF;
END;
$$
LANGUAGE plpgsql
RETURNS NULL ON NULL INPUT;  
  '''
curs.execute(create_threshold_functions)
conn.commit()
print('Done.')

# Restrict to indicators associated with study region (except distance to closest dest indicators)
ind_matrix = df_inds[df_inds['locale'].str.contains('|'.join([locale,'\*']))]

# Get a list of all potential destinations for distance to closest 
# (some may not be present in region, will be null, so we can refer to them in later queries)
# destination names
categories = [x for x in df_destinations.destination.tolist()]
category_list = ','.join(categories)
category_types = '"{}" int'.format('" int, "'.join(categories))

# destination classes
array_categories = [x for x in df_destinations.destination_class.tolist()]
array_category_list = ','.join(array_categories)
array_category_types = '"{}" int[]'.format('" int[], "'.join(array_categories))

print("Combine destination array and closest tables...")
# # Create schema for distances 
# sql = '''
# CREATE SCHEMA IF NOT EXISTS d_3200m_cl;
# -- CREATE INDEX od_3200m_dest_class_idx ON od_distances_3200m_20191129  (dest_class);
# -- CREATE INDEX od_closest_dest_class_idx ON od_distances_3200m_20191129  (dest_class);
# '''
# curs.execute(sql)
# conn.commit()
# Get a list of all potential destinations for distance to closest 
    
dest_list = df_destinations[['destination_class','working_table_2019']].set_index('destination_class')
timepoints = {'original':'','new':'_20191129'}
for d in dest_list.index:
    era = timepoints[dest_list.loc[d,'working_table_2019']]
    if era == timepoints['new']:
        sql = '''
          DROP TABLE IF EXISTS d_3200m_cl."{d}"
          '''.format(d = d)
        curs.execute(sql)
        conn.commit()
    print(' - {}'.format(d))
    sql = '''
        CREATE TABLE IF NOT EXISTS d_3200m_cl."{d}" AS 
        SELECT {id},
               array_append_if_gr(dests.distances,cl.distance) distances 
        FROM od_distances_3200m{era} dests
        LEFT JOIN od_closest{era} cl USING ({id},dest_class)
        WHERE dest_class = '{d}';
        CREATE UNIQUE INDEX IF NOT EXISTS {d}_idx ON  d_3200m_cl."{d}" ({id}); 
    '''.format(d = d,
               id = points_id,
               era = era)
    # print(sql)
    curs.execute(sql)
    conn.commit()

# Neighbourhood_indicators
print("Create nh_inds_distance (curated distance to closest table for re-use by other indicators)... "),
nh_distance = '''
DROP TABLE IF EXISTS {table};
CREATE TABLE IF NOT EXISTS {table} AS
SELECT 
       p.{id},
       LEAST(array_min(convenience_osm.distances),
             array_min(newsagent_osm.distances),
             array_min(petrolstation_osm.distances),
             array_min(market_osm.distances)) AS convenience_osm_2018,
       LEAST(array_min(supermarket.distances),
             array_min(supermarket_osm.distances)) AS supermarket_hlc_2017_osm_2018,
       LEAST(array_min(community_centre_osm.distances),
             array_min(hlc_2016_community_centres.distances)) AS community_centre_hlc_2016_osm_2018,
       LEAST(array_min(bakery_osm.distances),
             array_min(meat_seafood_osm.distances),
             array_min(fruit_veg_osm.distances),
             array_min(deli_osm.distances)) AS food_fresh_specialty_osm_2018,
       LEAST(array_min(fastfood_osm.distances),
             array_min(food_court_osm.distances),
             array_min(fast_food.distances)) AS food_fast_hlc_2017_osm_2018,         
       LEAST(array_min(restaurant_osm.distances),
             array_min(cafe_osm.distances),
             array_min(pub_osm.distances)) AS food_dining_osm_2018,     
       LEAST(array_min(museum_osm.distances), 
             array_min(theatre_osm.distances), 
             array_min(cinema_osm.distances), 
             array_min(art_gallery_osm.distances), 
             array_min(art_centre_osm.distances)) AS culture_osm_2018,            
       LEAST(array_min(bar_osm.distances), 
             array_min(nightclub_osm.distances),
             array_min(pub_osm.distances)) AS alcohol_nightlife_osm_2018,            
       LEAST(array_min("P_12_Schools_gov".distances), 
             array_min(primary_schools_gov.distances)) AS schools_primary_all_gov,           
       LEAST(array_min("P_12_Schools_gov".distances), 
             array_min(secondary_schools_gov.distances)) AS schools_secondary_all_gov,
       LEAST(array_min(gtfs_20191008_20191205_bus_0015.distances),
             array_min(gtfs_20191008_20191205_ferry_0015.distances),
             array_min(gtfs_20191008_20191205_train_0015.distances),
             array_min(gtfs_20191008_20191205_tram_0015.distances)) AS gtfs_20191008_20191205_frequent_pt_0015,
       LEAST(array_min(gtfs_20191008_20191205_bus_0030.distances),
             array_min(gtfs_20191008_20191205_ferry_0030.distances),
             array_min(gtfs_20191008_20191205_train_0030.distances),
             array_min(gtfs_20191008_20191205_tram_0030.distances)) AS gtfs_20191008_20191205_frequent_pt_0030,
       LEAST(array_min(gtfs_20191008_20191205_bus_0045.distances),
             array_min(gtfs_20191008_20191205_ferry_0045.distances),
             array_min(gtfs_20191008_20191205_train_0045.distances),
             array_min(gtfs_20191008_20191205_tram_0045.distances)) AS gtfs_20191008_20191205_frequent_pt_0045,
       LEAST(array_min(gtfs_20191008_20191205_bus_any.distances),
             array_min(gtfs_20191008_20191205_ferry_any.distances),
             array_min(gtfs_20191008_20191205_train_any.distances),
             array_min(gtfs_20191008_20191205_tram_any.distances)) AS gtfs_20191008_20191205_any_pt
FROM parcel_dwellings p
LEFT JOIN d_3200m_cl."P_12_Schools_gov"   ON p.{id}    = d_3200m_cl."P_12_Schools_gov".{id}
LEFT JOIN d_3200m_cl.primary_schools_gov   ON p.{id}    = d_3200m_cl.primary_schools_gov.{id}
LEFT JOIN d_3200m_cl.secondary_schools_gov   ON p.{id}    = d_3200m_cl.secondary_schools_gov.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_bus_0015   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_bus_0015.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_ferry_0015   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_ferry_0015.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_train_0015   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_train_0015.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_tram_0015   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_tram_0015.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_bus_0030   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_bus_0030.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_ferry_0030   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_ferry_0030.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_train_0030   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_train_0030.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_tram_0030   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_tram_0030.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_bus_0045   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_bus_0045.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_ferry_0045   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_ferry_0045.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_train_0045   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_train_0045.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_tram_0045   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_tram_0045.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_bus_any   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_bus_any.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_ferry_any   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_ferry_any.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_train_any   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_train_any.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_tram_any   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_tram_any.{id}
LEFT JOIN d_3200m_cl.convenience_osm   ON p.{id}    = d_3200m_cl.convenience_osm.{id}
LEFT JOIN d_3200m_cl.newsagent_osm     ON p.{id}    = d_3200m_cl.newsagent_osm.{id}
LEFT JOIN d_3200m_cl.petrolstation_osm ON p.{id}    = d_3200m_cl.petrolstation_osm.{id}
LEFT JOIN d_3200m_cl.market_osm        ON p.{id}    = d_3200m_cl.market_osm.{id}
LEFT JOIN d_3200m_cl.supermarket ON p.{id}    = d_3200m_cl.supermarket.{id}
LEFT JOIN d_3200m_cl.supermarket_osm ON p.{id} = d_3200m_cl.supermarket_osm.{id} 
LEFT JOIN d_3200m_cl.community_centre_osm ON p.{id} = d_3200m_cl.community_centre_osm.{id}
LEFT JOIN d_3200m_cl.hlc_2016_community_centres ON p.{id} = d_3200m_cl.hlc_2016_community_centres.{id}
LEFT JOIN d_3200m_cl.bakery_osm       ON p.{id} = d_3200m_cl.bakery_osm.{id}
LEFT JOIN d_3200m_cl.meat_seafood_osm ON p.{id} = d_3200m_cl.meat_seafood_osm.{id}
LEFT JOIN d_3200m_cl.fruit_veg_osm   ON p.{id}  = d_3200m_cl.fruit_veg_osm.{id}
LEFT JOIN d_3200m_cl.deli_osm        ON p.{id}  = d_3200m_cl.deli_osm.{id}
LEFT JOIN d_3200m_cl.fastfood_osm    ON p.{id}  = d_3200m_cl.fastfood_osm.{id}
LEFT JOIN d_3200m_cl.food_court_osm  ON p.{id}  = d_3200m_cl.food_court_osm.{id}            
LEFT JOIN d_3200m_cl.fast_food   ON p.{id}  = d_3200m_cl.fast_food.{id}
LEFT JOIN d_3200m_cl.restaurant_osm  ON p.{id}  = d_3200m_cl.restaurant_osm.{id}
LEFT JOIN d_3200m_cl.cafe_osm        ON p.{id}  = d_3200m_cl.cafe_osm.{id}
LEFT JOIN d_3200m_cl.museum_osm      ON p.{id}  = d_3200m_cl.museum_osm.{id} 
LEFT JOIN d_3200m_cl.theatre_osm     ON p.{id}  = d_3200m_cl.theatre_osm.{id} 
LEFT JOIN d_3200m_cl.cinema_osm      ON p.{id}  = d_3200m_cl.cinema_osm.{id} 
LEFT JOIN d_3200m_cl.art_gallery_osm ON p.{id}  = d_3200m_cl.art_gallery_osm.{id} 
LEFT JOIN d_3200m_cl.art_centre_osm  ON p.{id}  = d_3200m_cl.art_centre_osm.{id} 
LEFT JOIN d_3200m_cl.bar_osm         ON p.{id}  = d_3200m_cl.bar_osm.{id} 
LEFT JOIN d_3200m_cl.nightclub_osm   ON p.{id}  = d_3200m_cl.nightclub_osm.{id}
LEFT JOIN d_3200m_cl.pub_osm         ON p.{id}  = d_3200m_cl.pub_osm.{id}  
;
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
        threshold_{threshold_type}(convenience_osm_2018,{nh_threshold}) AS convenience_osm_2018, 
        threshold_{threshold_type}(supermarket_hlc_2017_osm_2018,{nh_threshold}) AS supermarket_hlc_2017_osm_2018          , 
        threshold_{threshold_type}(community_centre_hlc_2016_osm_2018,{nh_threshold}) AS community_centre_hlc_2016_osm_2018,
        threshold_{threshold_type}(food_fresh_specialty_osm_2018,{nh_threshold}) AS food_fresh_specialty_osm_2018 ,
        threshold_{threshold_type}(food_fast_hlc_2017_osm_2018,{nh_threshold}) AS food_fast_hlc_2017_osm_2018,
        threshold_{threshold_type}(food_dining_osm_2018          ,{nh_threshold}) AS food_dining_osm_2018,
        threshold_{threshold_type}(culture_osm_2018              ,{nh_threshold}) AS culture_osm_2018,
        threshold_{threshold_type}(alcohol_nightlife_osm_2018    ,{nh_threshold}) AS alcohol_nightlife_osm_2018    
        FROM nh_inds_distance ;
        CREATE UNIQUE INDEX IF NOT EXISTS nh_inds_{threshold_type}_{nh_threshold}m_idx ON  nh_inds_{threshold_type}_{nh_threshold}m ({id}); 
        '''.format(id = points_id.lower(),threshold_type = threshold_type, nh_threshold = nh_threshold)
        curs.execute(sql)
        conn.commit()
print("Done.")

print("Processing neighbourhood indicators:")
# Define table name and abbreviation
# This saves us having to retype these values, and allows the code to be more easily re-used
table = 'ind_daily_living'
abbrev = 'dl'
print(" - {}".format(table)),
ind_list = []
from_list = []
for t in ['hard','soft']:
    for d in [400,800,1000,1600]:
        inds = '''
          (COALESCE(nh_inds_{t}_{d}m.convenience_osm_2018,0) + 
           COALESCE(nh_inds_{t}_{d}m.supermarket_hlc_2017_osm_2018,0) + 
           COALESCE(threshold_{t}(array_min(d_3200m_cl.gtfs_2018_stops.distances),{d}),0)) AS {abbrev}_{t}_{d}m
        '''.format(t=t,d=d,abbrev=abbrev)
        ind_list  +=[inds]
        from_list +=['LEFT JOIN nh_inds_{t}_{d}m ON p.{id} = nh_inds_{t}_{d}m.{id}'.format(t=t,d=d,id=points_id)]
sql = '''
DROP TABLE IF EXISTS {table};
CREATE TABLE {table} AS
SELECT p.{id},
       {inds}
FROM parcel_dwellings p
     {from_list}
     LEFT JOIN d_3200m_cl.gtfs_2018_stops ON p.{id} = d_3200m_cl.gtfs_2018_stops.{id};
CREATE UNIQUE INDEX {table}_idx ON  {table} ({id});
'''.format(inds=','.join(ind_list), 
           from_list = '\r\n'.join(from_list),
           id = points_id,
           table = table)
curs.execute(sql)
conn.commit()
print(" Done.")

table = 'ind_local_living'
abbrev = 'll'
print(" - {}".format(table)),
ind_list = []
from_list = []
for t in ['hard','soft']:
    for d in [400,800,1000,1600]:
        inds = '''
            (COALESCE(nh_inds_{t}_{d}m.community_centre_hlc_2016_osm_2018,0) + 
            COALESCE(threshold_{t}(array_min(d_3200m_cl.libraries.distances),{d}),0) +
            COALESCE(threshold_{t}(array_min(d_3200m_cl.childcare_all_meet.distances),{d}),0) +
            COALESCE(threshold_{t}(array_min(d_3200m_cl. nhsd_2017_dentist.distances),{d}),0) +
            COALESCE(threshold_{t}(array_min(d_3200m_cl. nhsd_2017_gp.distances),{d}),0) +
            COALESCE(threshold_{t}(array_min(d_3200m_cl. nhsd_2017_pharmacy.distances),{d}),0) +
            COALESCE(nh_inds_{t}_{d}m.supermarket_hlc_2017_osm_2018,0) + 
            COALESCE(nh_inds_{t}_{d}m.convenience_osm_2018,0) +
            COALESCE(nh_inds_{t}_{d}m.food_fresh_specialty_osm_2018,0) +
            COALESCE(threshold_{t}(array_min(d_3200m_cl.postoffice_osm.distances),{d}),0) + 
            COALESCE(threshold_{t}(array_min(d_3200m_cl.gtfs_2018_stops.distances),{d}),0)) AS {abbrev}_{t}_{d}m
        '''.format(t=t,d=d,abbrev=abbrev)
        ind_list  +=[inds]
        from_list +=['LEFT JOIN nh_inds_{t}_{d}m ON p.{id} = nh_inds_{t}_{d}m.{id}'.format(t=t,d=d,id=points_id)]
sql = '''
DROP TABLE IF EXISTS {table};
CREATE TABLE {table} AS
SELECT p.{id},
       {inds}
FROM parcel_dwellings p
     {from_list}
    LEFT JOIN d_3200m_cl.libraries ON p.{id} = d_3200m_cl.libraries.{id}
    LEFT JOIN d_3200m_cl.childcare_all_meet ON p.{id} = d_3200m_cl.childcare_all_meet.{id}
    LEFT JOIN d_3200m_cl. nhsd_2017_dentist ON p.{id} = d_3200m_cl. nhsd_2017_dentist.{id}
    LEFT JOIN d_3200m_cl. nhsd_2017_gp ON p.{id} = d_3200m_cl. nhsd_2017_gp.{id}
    LEFT JOIN d_3200m_cl. nhsd_2017_pharmacy ON p.{id} = d_3200m_cl. nhsd_2017_pharmacy.{id}
    LEFT JOIN d_3200m_cl.postoffice_osm ON p.{id} = d_3200m_cl.postoffice_osm.{id} 
    LEFT JOIN d_3200m_cl.gtfs_2018_stops ON p.{id} = d_3200m_cl.gtfs_2018_stops.{id};
CREATE UNIQUE INDEX {table}_idx ON  {table} ({id});
'''.format(inds=','.join(ind_list), 
           from_list = '\r\n'.join(from_list),
           id = points_id,
           table = table)
curs.execute(sql)
conn.commit()
print(" Done.")
 
table = 'ind_si_mix'
abbrev = 'si'
print(" - {}".format(table)),

sql = '''
-- DROP TABLE IF EXISTS {table};
CREATE TABLE IF NOT EXISTS {table} AS
SELECT p.{id},
    (COALESCE(threshold_soft(nh_inds_distance.community_centre_hlc_2016_osm_2018, 1000),0) +
    COALESCE(threshold_soft(LEAST(array_min("museum_osm".distances),array_min("art_gallery_osm".distances)), 3200),0) +
    COALESCE(threshold_soft(LEAST(array_min("cinema_osm".distances),array_min("theatre_osm".distances)), 3200),0) +
    COALESCE(threshold_soft(array_min("libraries".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("childcare_oshc_meet".distances), 1600),0) +
    COALESCE(threshold_soft(array_min("childcare_all_meet".distances), 800),0)  +
    COALESCE(threshold_soft(nh_inds_distance.schools_primary_all_gov, 1600),0) +
    COALESCE(threshold_soft(nh_inds_distance.schools_primary_all_gov, 1600),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_aged_care_residential".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_pharmacy".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_mc_family_health".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_other_community_health_care".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_dentist".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_gp".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("public_swimming_pool_osm".distances), 1200),0) +
    COALESCE(threshold_soft(ind_os_distance.sport_distance_m, 1000),0)) AS si_mix
    FROM parcel_dwellings p
    LEFT JOIN nh_inds_distance ON p.{id} = nh_inds_distance.{id}
    LEFT JOIN d_3200m_cl."museum_osm" ON p.{id} = d_3200m_cl."museum_osm".{id}
    LEFT JOIN d_3200m_cl."art_gallery_osm" ON p.{id} = d_3200m_cl."art_gallery_osm".{id}
    LEFT JOIN d_3200m_cl."cinema_osm" ON p.{id} = d_3200m_cl."cinema_osm".{id}
    LEFT JOIN d_3200m_cl."theatre_osm" ON p.{id} = d_3200m_cl."theatre_osm".{id}
    LEFT JOIN d_3200m_cl."libraries" ON p.{id} = d_3200m_cl."libraries".{id}
    LEFT JOIN d_3200m_cl."childcare_oshc_meet" ON p.{id} = d_3200m_cl."childcare_oshc_meet".{id}
    LEFT JOIN d_3200m_cl."childcare_all_meet" ON p.{id} = d_3200m_cl."childcare_all_meet".{id}
    LEFT JOIN d_3200m_cl."nhsd_2017_aged_care_residential" ON p.{id} = d_3200m_cl."nhsd_2017_aged_care_residential".{id}
    LEFT JOIN d_3200m_cl."nhsd_2017_pharmacy" ON p.{id} = d_3200m_cl."nhsd_2017_pharmacy".{id}
    LEFT JOIN d_3200m_cl."nhsd_2017_mc_family_health" ON p.{id} = d_3200m_cl."nhsd_2017_mc_family_health".{id}
    LEFT JOIN d_3200m_cl."nhsd_2017_other_community_health_care" ON p.{id} = d_3200m_cl."nhsd_2017_other_community_health_care".{id}
    LEFT JOIN d_3200m_cl."nhsd_2017_dentist" ON p.{id} = d_3200m_cl."nhsd_2017_dentist".{id}
    LEFT JOIN d_3200m_cl."nhsd_2017_gp" ON p.{id} = d_3200m_cl."nhsd_2017_gp".{id}
    LEFT JOIN d_3200m_cl."public_swimming_pool_osm" ON p.{id} = d_3200m_cl."public_swimming_pool_osm".{id}
    LEFT JOIN ind_os_distance ON p.{id} = ind_os_distance.{id};
    CREATE UNIQUE INDEX IF NOT EXISTS {table}_idx ON  {table} ({id});
'''.format(id = points_id,
           table = table)
curs.execute(sql)
conn.commit()
print(" Done.")

# # The Urban Liveability Index


# # Read in indicator description matrix
# ind_matrix = df_inds
# uli = {}
# for ind in ['dwelling_density','street_connectivity','walkability','pt_freq_400m','pos_large_400m','supermarket_1km']:
  # suffix = ''
  # if ind in ['walkability','pt_freq_400m','pos_large_400m','supermarket_1km']:
    # suffix = '_soft'
  # uli[ind] = '{}{}'.format(ind_matrix.loc[ind_matrix['ind_plain']==ind,'ind'].values[0].encode('utf8'),suffix)


# # Restrict to indicators associated with study region
# ind_matrix = ind_matrix[ind_matrix['ind']=='uli']
# uli_locations = ind_matrix[ind_matrix['ind']=='uli']['locale'].iloc[0].encode('utf')
# if locale not in uli_locations and uli_locations != '*':
  # print("This location ('{locale}') is not marked for calculation of the Urban Liveability Index; check the indicator_setup file.".format(locale = locale))
  # sys.exit()

# conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
# curs = conn.cursor()  

# engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 # pwd  = db_pwd,
                                                                 # host = db_host,
                                                                 # db   = db))

# # Define function to shape if variable is outlying  
# createFunction = '''
  # -- outlier limiting/compressing function
  # -- if x < -2SD(x), scale up (hard knee upwards compression) to reach minimum by -3SD.
  # -- if x > 2SD(x), scale up (hard knee downwards compression) to reach maximum by 3SD(x).
  
  # CREATE OR REPLACE FUNCTION clean(var double precision,min_val double precision, max_val double precision, mean double precision, sd double precision) RETURNS double precision AS 
  # $$
  # DECLARE
  # ll double precision := mean - 2*sd;
  # ul double precision := mean + 2*sd;
  # c  double precision :=  1*sd;
  # BEGIN
    # IF (min_val < ll-c) AND (var < ll) THEN 
      # RETURN ll - c + c*(var - min_val)/(ll-min_val);
    # ELSIF (max_val > ul+c) AND (var > ul) THEN 
      # RETURN ul + c*(var - ul)/( max_val - ul );
    # ELSE 
      # RETURN var;
    # END IF;
  # END;
  # $$
  # LANGUAGE plpgsql
  # RETURNS NULL ON NULL INPUT;
  # '''
# curs.execute(createFunction)
# conn.commit()
# print("Created custom function.")


# # collate indicators for national liveability index

# sql = '''
# DROP TABLE IF EXISTS uli_inds ; 
# CREATE TABLE IF NOT EXISTS uli_inds AS
# SELECT p.{id},
    # COALESCE(sc_nh1600m,0) AS sc_nh1600m,
    # COALESCE(dd_nh1600m,0) AS dd_nh1600m,
   # (COALESCE(threshold_soft(nh_inds_distance.community_centre_hlc_2016_osm_2018, 1000),0) +
    # COALESCE(threshold_soft(LEAST(array_min("museum_osm".distances),array_min("art_gallery_osm".distances)), 3200),0) +
    # COALESCE(threshold_soft(LEAST(array_min("cinema_osm".distances),array_min("theatre_osm".distances)), 3200),0) +
    # COALESCE(threshold_soft(array_min("libraries".distances), 1000),0))/4.0 AS community_culture_leisure ,
   # (COALESCE(threshold_soft(array_min("childcare_oshc_meet".distances), 1600),0) +
    # COALESCE(threshold_soft(array_min("childcare_all_meet".distances), 800),0))/2.0 AS early_years,
   # (COALESCE(threshold_soft(nh_inds_distance.schools_primary_all_gov, 1600),0) +
    # COALESCE(threshold_soft(nh_inds_distance.schools_primary_all_gov, 1600),0))/2.0 AS education ,
   # (COALESCE(threshold_soft(array_min("nhsd_2017_aged_care_residential".distances), 1000),0) +
    # COALESCE(threshold_soft(array_min("nhsd_2017_pharmacy".distances), 1000),0) +
    # COALESCE(threshold_soft(array_min("nhsd_2017_mc_family_health".distances), 1000),0) +
    # COALESCE(threshold_soft(array_min("nhsd_2017_other_community_health_care".distances), 1000),0) +
    # COALESCE(threshold_soft(array_min("nhsd_2017_dentist".distances), 1000),0) +
    # COALESCE(threshold_soft(array_min("nhsd_2017_gp".distances), 1000),0))/6.0 AS health_services ,
   # (COALESCE(threshold_soft(array_min("public_swimming_pool_osm".distances), 1200),0) +
    # COALESCE(threshold_soft(ind_os_distance.sport_distance_m, 1000),0))/2.0 AS sport_rec,
   # (COALESCE(threshold_soft(array_min("fruit_veg_osm".distances), 1000),0) +
    # COALESCE(threshold_soft(array_min("meat_seafood_osm".distances), 3200),0) +
    # COALESCE(threshold_soft(array_min("supermarket_osm".distances), 1000),0))/3.0 AS food,    
   # (COALESCE(threshold_soft(array_min("convenience_osm".distances), 1000),0) +
    # COALESCE(threshold_soft(array_min("newsagent_osm".distances), 3200),0) +
    # COALESCE(threshold_soft(array_min("petrolstation_osm".distances), 1000),0))/3.0 AS convenience,         
    # COALESCE(threshold_soft(gtfs_20191008_20191205_frequent_pt_0030,400),0) AS pt_regular_400m,
    # COALESCE(threshold_soft(ind_os_distance.pos_15k_sqm_distance_m,400),0) AS pos_large_400m,
    # -- we coalesce 30:40 measures to 0, as nulls mean no one is in bottom two housing quintiles - really 0/0 implies 0% in this context
    # -- noting that null is not acceptable.  This should be discussed, but is workable for now.
    # -- Later, we reverse polarity of 30 40 measure
    # COALESCE(pcent_30_40,0) AS abs_30_40,
    # COALESCE(pct_live_work_local_area,0) AS abs_live_sa1_work_sa3
# FROM parcel_dwellings p
# LEFT JOIN area_linkage a ON p.mb_code_20 = a.mb_code_2016
# LEFT JOIN (SELECT DISTINCT({id}) FROM excluded_parcels) e ON p.{id} = e.{id}
# LEFT JOIN nh_inds_distance ON p.{id} = nh_inds_distance.{id}
# LEFT JOIN sc_nh1600m ON p.{id} = sc_nh1600m.{id}
# LEFT JOIN dd_nh1600m ON p.{id} = dd_nh1600m.{id}
# LEFT JOIN ind_os_distance ON p.{id} = ind_os_distance.{id}
# LEFT JOIN abs_ind_30_40 h ON a.sa1_7digitcode_2016 = h.sa1_7digitcode_2016::text
# LEFT JOIN live_sa1_work_sa3 l ON a.sa1_7digitcode_2016 = l.sa1_7digitcode_2016::text
# LEFT JOIN d_3200m_cl."fruit_veg_osm" ON p.{id} = d_3200m_cl."fruit_veg_osm".{id}
# LEFT JOIN d_3200m_cl."meat_seafood_osm" ON p.{id} = d_3200m_cl."meat_seafood_osm".{id}
# LEFT JOIN d_3200m_cl."supermarket_osm" ON p.{id} = d_3200m_cl."supermarket_osm".{id}
# LEFT JOIN d_3200m_cl."convenience_osm" ON p.{id} = d_3200m_cl."convenience_osm".{id}
# LEFT JOIN d_3200m_cl."newsagent_osm" ON p.{id} = d_3200m_cl."newsagent_osm".{id}
# LEFT JOIN d_3200m_cl."petrolstation_osm" ON p.{id} = d_3200m_cl."petrolstation_osm".{id}
# LEFT JOIN d_3200m_cl."museum_osm" ON p.{id} = d_3200m_cl."museum_osm".{id}
# LEFT JOIN d_3200m_cl."art_gallery_osm" ON p.{id} = d_3200m_cl."art_gallery_osm".{id}
# LEFT JOIN d_3200m_cl."cinema_osm" ON p.{id} = d_3200m_cl."cinema_osm".{id}
# LEFT JOIN d_3200m_cl."theatre_osm" ON p.{id} = d_3200m_cl."theatre_osm".{id}
# LEFT JOIN d_3200m_cl."libraries" ON p.{id} = d_3200m_cl."libraries".{id}
# LEFT JOIN d_3200m_cl."childcare_oshc_meet" ON p.{id} = d_3200m_cl."childcare_oshc_meet".{id}
# LEFT JOIN d_3200m_cl."childcare_all_meet" ON p.{id} = d_3200m_cl."childcare_all_meet".{id}
# LEFT JOIN d_3200m_cl."nhsd_2017_aged_care_residential" ON p.{id} = d_3200m_cl."nhsd_2017_aged_care_residential".{id}
# LEFT JOIN d_3200m_cl."nhsd_2017_pharmacy" ON p.{id} = d_3200m_cl."nhsd_2017_pharmacy".{id}
# LEFT JOIN d_3200m_cl."nhsd_2017_mc_family_health" ON p.{id} = d_3200m_cl."nhsd_2017_mc_family_health".{id}
# LEFT JOIN d_3200m_cl."nhsd_2017_other_community_health_care" ON p.{id} = d_3200m_cl."nhsd_2017_other_community_health_care".{id}
# LEFT JOIN d_3200m_cl."nhsd_2017_dentist" ON p.{id} = d_3200m_cl."nhsd_2017_dentist".{id}
# LEFT JOIN d_3200m_cl."nhsd_2017_gp" ON p.{id} = d_3200m_cl."nhsd_2017_gp".{id}
# LEFT JOIN d_3200m_cl."public_swimming_pool_osm" ON p.{id} = d_3200m_cl."public_swimming_pool_osm".{id}
# WHERE e.{id} IS NULL;
# CREATE UNIQUE INDEX IF NOT EXISTS ix_uli_inds ON  uli_inds ({id});
# '''.format(id = points_id)
# curs.execute(sql)
# conn.commit()
# print("Created liveability indicator table uli_inds.")

# # The below uses our custom clean function, drawing on (indicator, min, max, mean, sd)
# sql = '''
# DROP TABLE IF EXISTS uli_inds_clean ; 
# CREATE TABLE uli_inds_clean AS
# SELECT i.{id},
       # clean(i.sc_nh1600m               , s.sc_nh1600m[1]               , s.sc_nh1600m[2]                , s.sc_nh1600m[3]               , s.sc_nh1600m[4]               ) AS sc_nh1600m               ,
       # clean(i.dd_nh1600m               , s.dd_nh1600m[1]               , s.dd_nh1600m[2]                , s.dd_nh1600m[3]               , s.dd_nh1600m[4]               ) AS dd_nh1600m               ,
       # clean(i.community_culture_leisure, s.community_culture_leisure[1], s.community_culture_leisure[2] , s.community_culture_leisure[3], s.community_culture_leisure[4]) AS community_culture_leisure,
       # clean(i.early_years              , s.early_years[1]              , s.early_years[2]               , s.early_years[3]              , s.early_years[4]              ) AS early_years              ,
       # clean(i.education                , s.education[1]                , s.education[2]                 , s.education[3]                , s.education[4]                ) AS education                ,
       # clean(i.health_services          , s.health_services[1]          , s.health_services[2]           , s.health_services[3]          , s.health_services[4]          ) AS health_services          , 
       # clean(i.sport_rec                , s.sport_rec[1]                , s.sport_rec[2]                 , s.sport_rec[3]                , s.sport_rec[4]                ) AS sport_rec                , 
       # clean(i.food                     , s.food[1]                     , s.food[2]                      , s.food[3]                     , s.food[4]                     ) AS food                     , 
       # clean(i.convenience              , s.convenience[1]              , s.convenience[2]               , s.convenience[3]              , s.convenience[4]              ) AS convenience              , 
       # clean(i.pt_regular_400m          , s.pt_regular_400m[1]          , s.pt_regular_400m[2]           , s.pt_regular_400m[3]          , s.pt_regular_400m[4]          ) AS pt_regular_400m          , 
       # clean(i.pos_large_400m           , s.pos_large_400m[1]           , s.pos_large_400m[2]            , s.pos_large_400m[3]           , s.pos_large_400m[4]           ) AS pos_large_400m           , 
       # clean(i.abs_30_40                , s.abs_30_40[1]                , s.abs_30_40[2]                 , s.abs_30_40[3]                , s.abs_30_40[4]                ) AS abs_30_40                , 
       # clean(i.abs_live_sa1_work_sa3    , s.abs_live_sa1_work_sa3[1]    , s.abs_live_sa1_work_sa3[2]     , s.abs_live_sa1_work_sa3[3]    , s.abs_live_sa1_work_sa3[4]    ) AS abs_live_sa1_work_sa3     
# FROM uli_inds i,
# (SELECT ARRAY[MIN(sc_nh1600m               ),MAX(sc_nh1600m               ),AVG(sc_nh1600m               ),STDDEV(sc_nh1600m               )] AS sc_nh1600m               ,
        # ARRAY[MIN(dd_nh1600m               ),MAX(dd_nh1600m               ),AVG(dd_nh1600m               ),STDDEV(dd_nh1600m               )] AS dd_nh1600m               ,
        # ARRAY[MIN(community_culture_leisure),MAX(community_culture_leisure),AVG(community_culture_leisure),STDDEV(community_culture_leisure)] AS community_culture_leisure,
        # ARRAY[MIN(early_years              ),MAX(early_years              ),AVG(early_years              ),STDDEV(early_years              )] AS early_years              ,
        # ARRAY[MIN(education                ),MAX(education                ),AVG(education                ),STDDEV(education                )] AS education                ,
        # ARRAY[MIN(health_services          ),MAX(health_services          ),AVG(health_services          ),STDDEV(health_services          )] AS health_services          ,
        # ARRAY[MIN(sport_rec                ),MAX(sport_rec                ),AVG(sport_rec                ),STDDEV(sport_rec                )] AS sport_rec                ,
        # ARRAY[MIN(food                     ),MAX(food                     ),AVG(food                     ),STDDEV(food                     )] AS food                     ,
        # ARRAY[MIN(convenience              ),MAX(convenience              ),AVG(convenience              ),STDDEV(convenience              )] AS convenience              ,
        # ARRAY[MIN(pt_regular_400m          ),MAX(pt_regular_400m          ),AVG(pt_regular_400m          ),STDDEV(pt_regular_400m          )] AS pt_regular_400m          ,
        # ARRAY[MIN(pos_large_400m           ),MAX(pos_large_400m           ),AVG(pos_large_400m           ),STDDEV(pos_large_400m           )] AS pos_large_400m           ,
        # ARRAY[MIN(abs_30_40                ),MAX(abs_30_40                ),AVG(abs_30_40                ),STDDEV(abs_30_40                )] AS abs_30_40                ,
        # ARRAY[MIN(abs_live_sa1_work_sa3    ),MAX(abs_live_sa1_work_sa3    ),AVG(abs_live_sa1_work_sa3    ),STDDEV(abs_live_sa1_work_sa3    )] AS abs_live_sa1_work_sa3    
 # FROM uli_inds) s;
# ALTER TABLE uli_inds_clean ADD PRIMARY KEY ({id});
  # '''.format(id = points_id)
# curs.execute(sql)
# conn.commit()
# print("Created table 'uli_inds_clean'")


# sql = '''
# -- Note that in this normalisation stage, indicator polarity is adjusted for: ABS 30:40 measure has values substracted from 100, whilst positive indicators have them added.
# DROP TABLE IF EXISTS uli_inds_norm ; 
# CREATE TABLE uli_inds_norm AS    
# SELECT c.{id},
       # 100 + 10 * (c.sc_nh1600m               - s.sc_nh1600m[1]               ) / s.sc_nh1600m[2]                ::double precision AS sc_nh1600m               ,
       # 100 + 10 * (c.dd_nh1600m               - s.dd_nh1600m[1]               ) / s.dd_nh1600m[2]                ::double precision AS dd_nh1600m               ,
       # 100 + 10 * (c.community_culture_leisure- s.community_culture_leisure[1]) / s.community_culture_leisure[2] ::double precision AS community_culture_leisure,
       # 100 + 10 * (c.early_years              - s.early_years[1]              ) / s.early_years[2]               ::double precision AS early_years              ,
       # 100 + 10 * (c.education                - s.education[1]                ) / s.education[2]                 ::double precision AS education                ,
       # 100 + 10 * (c.health_services          - s.health_services[1]          ) / s.health_services[2]           ::double precision AS health_services          ,
       # 100 + 10 * (c.sport_rec                - s.sport_rec[1]                ) / s.sport_rec[2]                 ::double precision AS sport_rec                ,
       # 100 + 10 * (c.food                     - s.food[1]                     ) / s.food[2]                      ::double precision AS food                     ,
       # 100 + 10 * (c.convenience              - s.convenience[1]              ) / s.convenience[2]               ::double precision AS convenience              ,
       # 100 + 10 * (c.pt_regular_400m          - s.pt_regular_400m[1]          ) / s.pt_regular_400m[2]           ::double precision AS pt_regular_400m          ,
       # 100 + 10 * (c.pos_large_400m           - s.pos_large_400m[1]           ) / s.pos_large_400m[2]            ::double precision AS pos_large_400m           ,
       # 100 - 10 * (c.abs_30_40                - s.abs_30_40[1]                ) / s.abs_30_40[2]                 ::double precision AS abs_30_40                ,
       # 100 + 10 * (c.abs_live_sa1_work_sa3    - s.abs_live_sa1_work_sa3[1]    ) / s.abs_live_sa1_work_sa3[2]     ::double precision AS abs_live_sa1_work_sa3    
# FROM uli_inds_clean c,
# (SELECT ARRAY[AVG(sc_nh1600m               ),STDDEV(sc_nh1600m               )] AS sc_nh1600m               ,
        # ARRAY[AVG(dd_nh1600m               ),STDDEV(dd_nh1600m               )] AS dd_nh1600m               ,
        # ARRAY[AVG(community_culture_leisure),STDDEV(community_culture_leisure)] AS community_culture_leisure,
        # ARRAY[AVG(early_years              ),STDDEV(early_years              )] AS early_years              ,
        # ARRAY[AVG(education                ),STDDEV(education                )] AS education                ,
        # ARRAY[AVG(health_services          ),STDDEV(health_services          )] AS health_services          ,
        # ARRAY[AVG(sport_rec                ),STDDEV(sport_rec                )] AS sport_rec                ,
        # ARRAY[AVG(food                     ),STDDEV(food                     )] AS food                     ,
        # ARRAY[AVG(convenience              ),STDDEV(convenience              )] AS convenience              ,
        # ARRAY[AVG(pt_regular_400m          ),STDDEV(pt_regular_400m          )] AS pt_regular_400m          ,
        # ARRAY[AVG(pos_large_400m           ),STDDEV(pos_large_400m           )] AS pos_large_400m           ,
        # ARRAY[AVG(abs_30_40                ),STDDEV(abs_30_40                )] AS abs_30_40                ,
        # ARRAY[AVG(abs_live_sa1_work_sa3    ),STDDEV(abs_live_sa1_work_sa3    )] AS abs_live_sa1_work_sa3    
 # FROM uli_inds_clean) s;
# ALTER TABLE uli_inds_norm ADD PRIMARY KEY ({id});
# '''.format(id = points_id)

# curs.execute(sql)
# conn.commit()
# print("Created table 'uli_inds_norm', a table of MPI-normalised indicators.")
 
# sql = ''' 
# -- 2. Create ULI
# -- rowmean*(1-(rowsd(z_j)/rowmean(z_j))^2) AS mpi_est_j
# DROP TABLE IF EXISTS uli ; 
# CREATE TABLE uli AS
# SELECT {id}, 
       # AVG(val) AS mean, 
       # stddev_pop(val) AS sd, 
       # stddev_pop(val)/AVG(val) AS cv, 
       # AVG(val)-(stddev_pop(val)^2)/AVG(val) AS uli 
# FROM (SELECT {id}, 
             # unnest(array[sc_nh1600m               ,
                          # dd_nh1600m               ,
                          # community_culture_leisure,
                          # early_years              ,
                          # education                ,
                          # health_services          ,
                          # sport_rec                ,
                          # food                     ,
                          # convenience              ,
                          # pt_regular_400m          ,
                          # pos_large_400m           ,
                          # abs_30_40                ,
                          # abs_live_sa1_work_sa3    
                          # ]) as val 
      # FROM uli_inds_norm ) alias
# GROUP BY {id};
# ALTER TABLE uli ADD PRIMARY KEY ({id});
# '''.format(id = points_id)

# curs.execute(sql)
# conn.commit()
# print("Created table 'uli', containing parcel level urban liveability index estimates, along with its required summary ingredients (mean, sd, coefficient of variation).")


# sql = '''
# SELECT 
# uli.uli
# ind_walkability.wa_soft_1600m
# d_3200m_cl.activity_centres.array_min(distances)
# d_3200m_cl.alcohol_offlicence.array_min(distances)
# ind_si_mix.si_mix
# nh_inds_distance.threshold_hard(gtfs_20191008_20191205_frequent_pt_0030,400)
# ind_os_distance.threshold_hard(pos_15k_sqm_distance_m,400)
# live_sa1_work_sa3.pct_live_work_local_area
# abs_ind_30_40.pcent_30_40
# FROM 
# uli
# ind_walkability
# d_3200m_cl.activity_centres
# d_3200m_cl.alcohol_offlicence
# ind_si_mix
# nh_inds_distance
# ind_os_distance
# live_sa1_work_sa3
# abs_ind_30_40
# '''


# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
