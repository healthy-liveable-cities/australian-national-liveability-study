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
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

# schema where point indicator output tables will be stored
schema = ind_point_schema

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
  DROP TABLE IF EXISTS {abbrev}_dest_counts;
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

print("Check that there are not entries in the 'log_od_distances' table which are not present as results in the 'od_closest' table... ")
# Check if the table exists; if it does, these areas have previously been re-imported, so no need to re-do
curs.execute('''SELECT DISTINCT(dest_name) FROM log_od_distances WHERE dest_name NOT IN (SELECT DISTINCT(dest_name) FROM od_closest);''')
orphan_destinations = [x[0] for x in curs.fetchall()]
if len(orphan_destinations) > 0:
    print('''
    Note: The following entries in your 'log_od_distances' table 
    (which logs destinations processed in hexes for distance to closest analysis)
    are not present in the od_closest table:
    {}
    The non-presence of these records is suprising and may relate to an error in processing
    at some stage in the workflow.  
    
    These records will now be removed from the log_od_distances table, and it is recommended 
    that you re-run the script distance to closest analysis script (at the time of writing,
    16_od_distances_closest_in_study_region.py) for this study region.
    '''.format('\n'.join(orphan_destinations)))
    curs.execute('''DELETE FROM log_od_distances WHERE dest_name NOT IN (SELECT DISTINCT(dest_name) FROM od_closest);''')
    conn.commit()
else:
    print("  - all good!")
    
print("Create summary table of destination distances (dest_distance_m)... ")
wide_table = 'dest_distance_m'
long_table = 'od_closest'
destination_names = ',\n'.join(['''MIN(CASE WHEN dest_name = '{d}' THEN distance END) AS "{d}"'''.format(d = d) for d in categories])
sql = '''
DROP TABLE IF EXISTS {wide_table}_old;
ALTER TABLE IF EXISTS {wide_table} RENAME TO {wide_table}_old;
ALTER INDEX IF EXISTS {wide_table}_idx RENAME TO {wide_table}_old_idx;
CREATE TABLE {wide_table} AS
SELECT {id},
       {destinations}
FROM {long_table}
GROUP BY {id};
CREATE UNIQUE INDEX IF NOT EXISTS {wide_table}_idx ON {wide_table} ({id});
'''.format(wide_table = wide_table,
           long_table = long_table,
           id = points_id.lower(),
           destinations = destination_names)
curs.execute(sql)
conn.commit()
print("Done.")

print("Create summary table of distances to destinations in 3200 metres (dest_distances_3200m)... "),
wide_table = 'dest_distances_3200m'
long_table = 'od_distances_3200m'
destination_classes = ',\n'.join(['''MIN(CASE WHEN dest_class = '{d}' THEN distances END) AS "{d}"'''.format(d = d) for d in array_categories])
sql = '''
DROP TABLE IF EXISTS {wide_table}_old;
ALTER TABLE IF EXISTS {wide_table} RENAME TO {wide_table}_old;
ALTER INDEX IF EXISTS {wide_table}_idx RENAME TO {wide_table}_old_idx;
CREATE TABLE {wide_table} AS
SELECT {id},
       {destinations}
FROM {long_table}
GROUP BY {id};
CREATE UNIQUE INDEX IF NOT EXISTS {wide_table}_idx ON {wide_table} ({id});
'''.format(wide_table = wide_table,
           long_table = long_table,
           id = points_id.lower(),
           destinations = destination_classes)
curs.execute(sql)
conn.commit()
print("Done.")

print("Combine destination array and closest tables..."),
# Get a list of all potential destinations for distance to closest 
# (some may not be present in region, will be null, so we can refer to them in later queries)
# destination names
categories = [x for x in df_destinations.destination.tolist()]
array_categories = [x for x in df_destinations.destination_class.tolist()]
dest_tuples = ',\n'.join(['''array_append_if_gr(dests."{0}",cl."{1}") AS "{0}"'''.format(*dest) for dest in zip(array_categories,categories)])
table = 'dest_distances_cl_3200m'
curs.execute('''
DROP TABLE IF EXISTS {table};
CREATE TABLE {table} AS
SELECT {id}, 
       {destinations}
FROM dest_distances_3200m dests 
LEFT JOIN dest_distance_m cl
USING ({id});
CREATE UNIQUE INDEX IF NOT EXISTS {table}_idx ON  {table} ({id}); 
'''.format(table = table,
           id = points_id.lower(),
           destinations = dest_tuples))
conn.commit()
print(" Done.")

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

print("Processing neighbourhood indicators:")
# Define table name and abbreviation
# This saves us having to retype these values, and allows the code to be more easily re-used
table = ['ind_daily_living','dl']
print(" - {table}".format(table = table[0])),
curs.execute('''SELECT 1 WHERE to_regclass('public.{table}') IS NOT NULL;'''.format(table = table[0]))
res = curs.fetchone()
if res:
    print("Table exists.")
if res is None:
    create_table = '''DROP TABLE IF EXISTS {table}; CREATE TABLE {table} AS SELECT {id} FROM sample_point_feature;'''.format(table = table[0], id = points_id.lower())
    curs.execute(create_table)
    conn.commit()
    for threshold_type in ['hard','soft']:
        for nh_threshold in [400,800,1000,1600]:
            populate_table = '''
            -- Note that we take NULL for distance to closest in this context to mean absence of presence
            -- Error checking at other stages of processing should confirm whether this is the case.
            ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {abbrev}_{threshold_type}_{nh_threshold}m float;
            UPDATE {table} t SET 
            {abbrev}_{threshold_type}_{nh_threshold}m = COALESCE(convenience_osm_2018,0) + 
                                                        GREATEST(COALESCE(supermarket_hlc_2017,0),COALESCE(supermarket_osm_2018,0)) + 
                                                        COALESCE(pt_any_gtfs_hlc_2018,0)
            FROM nh_inds_{threshold_type}_{nh_threshold}m nh
            WHERE t.{id} = nh.{id};
            '''.format(table = table[0], 
                    abbrev = table[1], 
                    id = points_id.lower(),
                    threshold_type = threshold_type, 
                    nh_threshold = nh_threshold)
            curs.execute(populate_table)
            conn.commit()
            print("."),
    create_index = '''CREATE UNIQUE INDEX {table}_idx ON  {table} ({id});  '''.format(table = table[0], id = points_id.lower())
    curs.execute(create_index)
    conn.commit()
    print(" Done.")

table = ['ind_local_living','ll']
print(" - {table}".format(table = table[0])),
curs.execute('''SELECT 1 WHERE to_regclass('public.{table}') IS NOT NULL;'''.format(table = table[0]))
res = curs.fetchone()
if res:
    print("Table exists.")
if res is None:
    create_table = '''DROP TABLE IF EXISTS {table}; CREATE TABLE {table} AS SELECT {id} FROM sample_point_feature;'''.format(table = table[0], id = points_id.lower())
    curs.execute(create_table)
    conn.commit()
    
    for threshold_type in ['hard','soft']:
        for nh_threshold in [400,800,1000,1600]:
            populate_table = '''
            -- Note that we take NULL for distance to closest in this context to mean absence of presence
            -- Error checking at other stages of processing should confirm whether this is the case.
            ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {abbrev}_{threshold_type}_{nh_threshold}m float;
            UPDATE {table} t SET 
            {abbrev}_{threshold_type}_{nh_threshold}m = COALESCE(community_pow_osm_2018,0) + 
                                                        COALESCE(libraries_hlc_2018,0) +
                                                        COALESCE(childcare_meets_acequa_2019,0) +
                                                        COALESCE(dentist_nhsd_2017,0) +
                                                        COALESCE(gp_nhsd_2017,0) +
                                                        COALESCE(pharmacy_nhsd_2017,0) +
                                                        GREATEST(COALESCE(supermarket_hlc_2017,0),COALESCE(supermarket_osm_2018,0)) + 
                                                        COALESCE(convenience_osm_2018,0) +
                                                        COALESCE(food_fresh_specialty_osm_2018,0) +
                                                        COALESCE(postoffice_osm_2018,0) + 
                                                        COALESCE(pt_any_gtfs_hlc_2018,0)
            FROM nh_inds_{threshold_type}_{nh_threshold}m nh
            WHERE t.{id} = nh.{id};
            '''.format(table = table[0], 
                    abbrev = table[1], 
                    id = points_id.lower(),
                    threshold_type = threshold_type, 
                    nh_threshold = nh_threshold)
            curs.execute(populate_table)
            conn.commit()
            print("."),
    create_index = '''CREATE UNIQUE INDEX {table}_idx ON  {table} ({id});  '''.format(table = table[0], id = points_id.lower())
    curs.execute(create_index)
    conn.commit()
    print(" Done.")
    

table = ['ind_walkability','wa']
print(" - {table}".format(table = table[0])),
curs.execute('''SELECT 1 WHERE to_regclass('public.{table}') IS NOT NULL;'''.format(table = table[0]))
res = curs.fetchone()
if res:
    print("Table exists.")
if res is None:
    create_table = '''DROP TABLE IF EXISTS {table}; CREATE TABLE {table} AS SELECT {id} FROM sample_point_feature;'''.format(table = table[0], id = points_id.lower())
    curs.execute(create_table)
    conn.commit()
    # we just calculate walkability at 1600m, so we'll set nh_threshold to that value
    nh_threshold = 1600
    for threshold_type in ['hard','soft']:
        populate_table = '''
        -- Note that we take NULL for distance to closest in this context to mean absence of presence
        -- Error checking at other stages of processing should confirm whether this is the case.
        ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {abbrev}_{threshold_type}_{nh_threshold}m float;
        UPDATE {table} t SET 
        {abbrev}_{threshold_type}_{nh_threshold}m = dl.z_dl + sc.z_sc + dd.z_dd
        FROM (SELECT {id}, (dl_{threshold_type}_{nh_threshold}m - AVG(dl_{threshold_type}_{nh_threshold}m) OVER())/stddev_pop(dl_{threshold_type}_{nh_threshold}m) OVER() as z_dl FROM ind_daily_living) dl
        LEFT JOIN (SELECT {id}, (sc_nh1600m - AVG(sc_nh1600m) OVER())/stddev_pop(sc_nh1600m) OVER() as z_sc FROM sc_nh1600m) sc ON sc.{id} = dl.{id}
        LEFT JOIN (SELECT {id}, (dd_nh1600m - AVG(dd_nh1600m) OVER())/stddev_pop(dd_nh1600m) OVER() as z_dd FROM dd_nh1600m) dd ON dd.{id} = dl.{id}
        WHERE t.{id} = dl.{id};
        '''.format(table = table[0], 
                   abbrev = table[1], 
                   id = points_id.lower(),
                   threshold_type = threshold_type, 
                   nh_threshold = nh_threshold)
        curs.execute(populate_table)
        conn.commit()
        print("."),
    create_index = '''CREATE UNIQUE INDEX {table}_idx ON  {table} ({id});  '''.format(table = table[0], id = points_id.lower())
    curs.execute(create_index)
    conn.commit()
    print(" Done.")

# calculate food indicators at both 1600 m and 3200 m 
print(" - ind_food... "),
# curs.execute('''SELECT 1 WHERE to_regclass('public.{table}') IS NOT NULL;'''.format(table = 'ind_food'))
# res = curs.fetchone()
# I am forcing the recreation of this table 
res = False
if res:
    print("Table exists.")
else:
    for nh_threshold in [1600,3200]:
        table = ['ind_food_{nh_threshold}m'.format(nh_threshold = nh_threshold),'f']
        sql = '''
        DROP TABLE IF EXISTS {table};
        CREATE TABLE IF NOT EXISTS {table} AS
        SELECT
            {id},
            d.supermarkets             AS food_count_supermarkets_{nh_threshold}m     ,
            d.fruit_veg                AS food_count_fruit_veg_{nh_threshold}m        ,
            d.specialty                AS food_count_other_specialty_{nh_threshold}m  ,
            d.supermarkets+d.fruit_veg AS food_count_healthier_{nh_threshold}m  ,
            d.fastfood                 AS food_count_fastfood_{nh_threshold}m       ,
            100 * ((d.supermarkets+d.fruit_veg)             
                    / NULLIF((d.supermarkets+d.fruit_veg+ d.fastfood):: float,0)) AS food_healthy_percent_{nh_threshold}m,
            (d.supermarkets+d.fruit_veg) / NULLIF(d.fastfood:: float,0) AS food_healthy_ratio_{nh_threshold}m,
            100 * ((d.supermarkets+d.fruit_veg+d.specialty) 
                    / NULLIF((d.supermarkets+d.fruit_veg+ d.fastfood++d.specialty):: float,0)) AS food_fresh_percent_{nh_threshold}m,
            (d.supermarkets+d.fruit_veg+d.specialty) / NULLIF(d.fastfood:: float,0) AS food_fresh_ratio_{nh_threshold}m,
            (d.supermarkets+d.fruit_veg+d.fastfood = 0)::int AS no_healthy_unhealthy_food_{nh_threshold}m,
            (d.supermarkets+d.fruit_veg+d.fastfood+d.specialty = 0)::int AS no_food_{nh_threshold}m
        FROM (SELECT 
                {id},
                GREATEST(COALESCE(count_in_threshold(supermarket,{nh_threshold}),0),
                        COALESCE(count_in_threshold(supermarket_osm,{nh_threshold}),0)) AS supermarkets,
                COALESCE(count_in_threshold(fruit_veg_osm,{nh_threshold}),0) AS fruit_veg,
                (COALESCE(count_in_threshold(bakery_osm,{nh_threshold}),0) +       
                COALESCE(count_in_threshold(meat_seafood_osm,{nh_threshold}),0) +          
                COALESCE(count_in_threshold(deli_osm,{nh_threshold}),0)) AS specialty,         
                GREATEST(COALESCE(count_in_threshold(fast_food,{nh_threshold}),0),
                        COALESCE(count_in_threshold(fastfood_osm,{nh_threshold}),0)) AS fastfood
            FROM dest_distances_3200m) d
        '''.format(table = table[0], 
                id = points_id.lower(),
                nh_threshold = nh_threshold)
        curs.execute(sql)
        conn.commit()
        create_index = '''CREATE UNIQUE INDEX IF NOT EXISTS {table}_idx ON  {table} ({id});  '''.format(table = table[0],id = points_id.lower())
        curs.execute(create_index)
        conn.commit()
    
    # combine food tables
    sql = '''
    DROP TABLE IF EXISTS ind_food;
    CREATE TABLE IF NOT EXISTS ind_food AS
    SELECT * FROM ind_food_1600m LEFT JOIN ind_food_3200m USING (gnaf_pid);
    CREATE UNIQUE INDEX IF NOT EXISTS ind_food_idx ON  ind_food ({id});
    DROP TABLE IF EXISTS ind_food_1600m;
    DROP TABLE IF EXISTS ind_food_3200m;
    '''.format(id = points_id.lower())
    curs.execute(sql)
    conn.commit()
    print(" Done.")

# Create Open Space measures (distances, which can later be considered with regard to thresholds)
# In addition to public open space (pos), also includes sport areas and blue space
table = ['ind_os_distances_3200m','os']
print(" - {table}".format(table = table[0])),

sql = '''
CREATE TABLE IF NOT EXISTS {table} AS SELECT {id} FROM sample_point_feature;
'''.format(table = table[0], id = points_id.lower())
curs.execute(sql)
conn.commit()

# Create OS measures if not existing
aos_of_interest = [["pos_any"         ,"aos_ha_public > 0"                         ],
                   ["pos_0k_4k_sqm"   ,"aos_ha_public > 0 AND aos_ha_public <= 0.4"],
                   ["pos_4k_sqm"      ,"aos_ha_public > 0.4"                       ],
                   ["pos_5k_sqm"      ,"aos_ha_public > 0.5"                       ],
                   ["pos_15k_sqm"     ,"aos_ha_public > 1.5"                       ],
                   ["pos_20k_sqm"     ,"aos_ha_public > 2"                         ],
                   ["pos_4k_10k_sqm"  ,"aos_ha_public > 0.4 AND aos_ha_public <= 1"],
                   ["pos_10k_50k_sqm" ,"aos_ha_public > 1 AND aos_ha_public <= 5"  ],
                   ["pos_50k_200k_sqm","aos_ha_public > 5 AND aos_ha_public <= 20" ],
                   ["pos_50k_sqm"     ,"aos_ha_public > 5 AND aos_ha_public <= 20" ],
                   ["pos_200k_sqm"    ,"aos_ha_public > 20"                        ]]

for aos in aos_of_interest:
    measure = '{}_distances_3200m'.format(aos[0])
    sql = '''
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name='{table}' and column_name='{column}';
    '''.format(table = table[0],column = measure)
    curs.execute(sql)
    res = curs.fetchone()
    if not res:   
        add_and_update_measure = '''
        DROP INDEX IF EXISTS {table}_idx;
        ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {measure} int[];
        UPDATE {table} t 
        SET {measure} = os_filtered.distances
        FROM sample_point_feature orig
        LEFT JOIN (SELECT p.{id}, 
                        array_agg(distance) AS distances
                    FROM sample_point_feature p
                    LEFT JOIN 
                    (SELECT {id},
                            (obj->>'aos_id')::int AS aos_id,
                            (obj->>'distance')::int AS distance
                    FROM od_aos_jsonb,
                        jsonb_array_elements(attributes) obj) o ON p.{id} = o.{id}
                    LEFT JOIN open_space_areas pos ON o.aos_id = pos.aos_id
                        WHERE pos.aos_id IS NOT NULL
                        AND {where}
                    GROUP BY p.{id}) os_filtered ON orig.{id} = os_filtered.{id}
        WHERE t.{id} = orig.{id};
        '''.format(id = points_id, table = table[0], measure = measure,where = aos[1])
        curs.execute(add_and_update_measure)
        conn.commit()
    print("."),

measure = 'sport_distances_3200m'
sql = '''
SELECT column_name 
FROM information_schema.columns 
WHERE table_name='{table}' and column_name='{column}';
'''.format(table = table[0],column = measure)
curs.execute(sql)
res = curs.fetchone()
if not res:     
    add_and_update_measure = '''
    DROP INDEX IF EXISTS {table}_idx;
    ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {measure} int[];
    UPDATE {table} t 
    SET {measure} = os_filtered.distances
    FROM sample_point_feature orig
    LEFT JOIN (SELECT p.{id}, 
                    array_agg(distance) AS distances
            FROM sample_point_feature p
            LEFT JOIN (SELECT {id},
                                (obj->>'aos_id')::int AS aos_id,
                                (obj->>'distance')::int AS distance
                            FROM od_aos_jsonb,
                                jsonb_array_elements(attributes) obj
                        WHERE (obj->>'distance')::int < 3200) o ON p.{id} = o.{id}                  
            WHERE EXISTS -- we restrict our results to distances to AOS with sports facilities 
                        (SELECT 1 FROM open_space_areas sport,
                                        jsonb_array_elements(attributes) obj
                            WHERE (obj->>'leisure' IN ('golf_course','sports_club','sports_centre','fitness_centre','pitch','track','fitness_station','ice_rink','swimming_pool') 
                            OR (obj->>'sport' IS NOT NULL 
                            AND obj->>'sport' != 'no'))
                            AND  o.aos_id = sport.aos_id)
            GROUP BY p.{id} ) os_filtered ON orig.{id} = os_filtered.{id}
    WHERE t.{id} = orig.{id};
    '''.format(id = points_id, table = table[0], measure = measure)
    curs.execute(add_and_update_measure)
    conn.commit()
print("."),


measure = 'pos_toilet_distances_3200m'
sql = '''
SELECT column_name 
FROM information_schema.columns 
WHERE table_name='{table}' and column_name='{column}';
'''.format(table = table[0],column = measure)
curs.execute(sql)
res = curs.fetchone()
if not res:     
    add_and_update_measure = '''
    DROP INDEX IF EXISTS {table}_idx;
    ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {measure} int[];
    UPDATE {table} t 
    SET {measure} = os_filtered.distances
    FROM sample_point_feature orig
    LEFT JOIN (SELECT DISTINCT ON (p.{id}) 
                    p.{id}, 
                    array_agg(distance) AS distances
            FROM sample_point_feature p
            LEFT JOIN   
                        (SELECT {id},  
                        (obj->>'aos_id')::int AS aos_id, 
                        (obj->>'distance')::int AS distance 
                        FROM od_aos_jsonb, 
                        jsonb_array_elements(attributes) obj) o ON p.{id} = o.{id} 
            LEFT JOIN open_space_areas pos ON o.aos_id = pos.aos_id 
                WHERE pos.aos_id IS NOT NULL  
                    AND co_location_100m ? 'toilets'
            GROUP BY p.{id} ) os_filtered ON orig.{id} = os_filtered.{id}
    WHERE t.{id} = orig.{id};
    '''.format(id = points_id, table = table[0], measure = measure)
    curs.execute(add_and_update_measure)
    conn.commit()
print("."),
print(" Done.")

create_index = '''CREATE UNIQUE INDEX IF NOT EXISTS {table}_idx ON  {table} ({id});  '''.format(table = table[0], id = points_id.lower())
curs.execute(create_index)
conn.commit()

table = ['ind_os_distance','os']
print(" - {table}".format(table = table[0])),    
sql = '''
DROP TABLE IF EXISTS ind_os_distance;
CREATE TABLE IF NOT EXISTS ind_os_distance AS
SELECT 
    {id},
    array_min(pos_any_distances_3200m) AS pos_any_distance_m,
    array_min(pos_0k_4k_sqm_distances_3200m) AS pos_0k_4k_sqm_distance_m,
    array_min(pos_4k_sqm_distances_3200m) AS pos_4k_sqm_distance_m,
    array_min(pos_5k_sqm_distances_3200m) AS pos_5k_sqm_distance_m,
    array_min(pos_15k_sqm_distances_3200m) AS pos_15k_sqm_distance_m,
    array_min(pos_20k_sqm_distances_3200m) AS pos_20k_sqm_distance_m,
    array_min(pos_4k_10k_sqm_distances_3200m) AS pos_4k_10k_sqm_distance_m,
    array_min(pos_10k_50k_sqm_distances_3200m) AS pos_10k_50k_sqm_distance_m,
    array_min(pos_50k_200k_sqm_distances_3200m) AS pos_50k_200k_sqm_distance_m,
    array_min(pos_50k_sqm_distances_3200m) AS pos_50k_sqm_distance_m,
    array_min(pos_200k_sqm_distances_3200m) AS pos_200k_sqm_distance_m,
    array_min(sport_distances_3200m) AS sport_distance_m,
    array_min(pos_toilet_distances_3200m) AS pos_toilet_distance_m
FROM ind_os_distances_3200m;
'''.format(id = points_id.lower())    
curs.execute(sql)
conn.commit()
create_index = '''CREATE UNIQUE INDEX IF NOT EXISTS {table}_idx ON  {table} ({id});  '''.format(table = table[0], id = points_id.lower())
curs.execute(create_index)
conn.commit()
print("Done.")
    
print("Import ACARA schools look up table with NAPLAN results and area linkage codes... "),
# Check if the table main_mb_2016_aust_full exists; if it does, these areas have previously been re-imported, so no need to re-do
command = (
          ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
          ' PG:"host={host} port=5432 dbname={db}'
          ' user={user} password = {pwd}" '
          ' {gpkg} "{feature}" '
          ' -nln {name} '
          ' -lco geometry_name="geom" '
          ' -lco  FID=acara_school_id ' .format(host = db_host,
                                                db = db,
                                                user = db_user,
                                                pwd = db_pwd,
                                                gpkg = os.path.join(folderPath,school_ratings),
                                                feature =  school_table,
                                                name =  school_table) 
          )
# print(command)
sp.call(command, shell=True) 

print("Create Open space areas - ACARA / NAPLAN linkage table for school queries... "),
sql = '''
--DROP TABLE IF EXISTS aos_acara_naplan;
CREATE TABLE IF NOT EXISTS aos_acara_naplan AS 
SELECT  DISTINCT ON (aos_id,acara_school_id)
       aos_id,  
       os_acara.acara_school_id, 
       -- get the seperate potential naplan scores (not all are recorded)
       year3_reading , 
       year3_writing , 
       year3_spelling, 
       year3_grammar , 
       year3_numeracy, 
       year5_reading , 
       year5_writing , 
       year5_spelling, 
       year5_grammar , 
       year5_numeracy, 
       year7_reading , 
       year7_writing , 
       year7_spelling, 
       year7_grammar , 
       year7_numeracy, 
       year9_reading , 
       year9_writing , 
       year9_spelling, 
       year9_grammar , 
       year9_numeracy, 
       -- take the sum of naplan scores for a school
       COALESCE(year3_reading ,0)+ 
       COALESCE(year3_writing ,0)+ 
       COALESCE(year3_spelling,0)+ 
       COALESCE(year3_grammar ,0)+ 
       COALESCE(year3_numeracy,0)+ 
       COALESCE(year5_reading ,0)+ 
       COALESCE(year5_writing ,0)+ 
       COALESCE(year5_spelling,0)+ 
       COALESCE(year5_grammar ,0)+ 
       COALESCE(year5_numeracy,0)+ 
       COALESCE(year7_reading ,0)+ 
       COALESCE(year7_writing ,0)+ 
       COALESCE(year7_spelling,0)+ 
       COALESCE(year7_grammar ,0)+ 
       COALESCE(year7_numeracy,0)+ 
       COALESCE(year9_reading ,0)+ 
       COALESCE(year9_writing ,0)+ 
       COALESCE(year9_spelling,0)+ 
       COALESCE(year9_grammar ,0)+ 
       COALESCE(year9_numeracy,0) AS sum, 
       -- take the non-null count of naplan scores for a school
       (select count(*) 
        from (values  
                    (year3_reading ), 
                    (year3_writing ), 
                    (year3_spelling), 
                    (year3_grammar ), 
                    (year3_numeracy), 
                    (year5_reading ), 
                    (year5_writing ), 
                    (year5_spelling), 
                    (year5_grammar ), 
                    (year5_numeracy), 
                    (year7_reading ), 
                    (year7_writing ), 
                    (year7_spelling), 
                    (year7_grammar ), 
                    (year7_numeracy), 
                    (year9_reading ), 
                    (year9_writing ), 
                    (year9_spelling), 
                    (year9_grammar ), 
                    (year9_numeracy) 
        ) as v(col) 
        where v.col is not null 
       ) as non_null_count 
FROM     
   -- extract school ids from open space table 
   (SELECT aos_id,  
         (tags.value->>'acara_scho')::int AS acara_school_id  
    FROM open_space_areas schools, 
         jsonb_array_elements(schools.attributes) obj, 
         jsonb_array_elements((obj ->>'school_tags')::jsonb) tags) os_acara 
    -- join schools with their naplan scores 
LEFT JOIN {table} ON os_acara.acara_school_id = {table}.acara_school_id 
WHERE os_acara.acara_school_id IS NOT NULL; 
-- create index 
CREATE UNIQUE INDEX IF NOT EXISTS aos_acara_naplan_idx ON  aos_acara_naplan (aos_id,acara_school_id);  
'''.format(table = school_table)
curs.execute(sql)
conn.commit()
print("Done.")

for nh_distance in [800,1600]:
    print(" Get NAPLAN average of all schools within {}m of address... ".format(nh_distance)),
    sql = '''
    CREATE TABLE IF NOT EXISTS ind_school_naplan_avg_{nh_distance}m AS
    SELECT p.{id},  
        COUNT(acara_school_id) AS school_count_{nh_distance}m, 
        AVG(o.distance)::int AS average_distance_{nh_distance}m, 
        AVG(sum) AS average_sum_of_naplan_{nh_distance}m, 
        AVG(non_null_count) AS average_test_count_{nh_distance}m, 
        AVG(sum/ nullif(non_null_count::float,0)) AS naplan_average_{nh_distance}m 
    FROM sample_point_feature p 
    LEFT JOIN  
        -- get the distances and ids for all AOS with schools within 3200 m
        (SELECT {id}, 
                (obj->>'aos_id')::int AS aos_id, 
                (obj->>'distance')::int AS distance 
        FROM od_aos_jsonb, 
            jsonb_array_elements(attributes) obj) o  
    ON p.{id} = o.{id} 
    LEFT JOIN aos_acara_naplan naplan ON o.aos_id = naplan.aos_id 
    WHERE naplan.acara_school_id IS NOT NULL 
        AND o.distance < {nh_distance}
    GROUP BY p.{id} ;
    -- create index 
    CREATE UNIQUE INDEX IF NOT EXISTS ind_school_naplan_avg_{nh_distance}m_idx
                     ON  ind_school_naplan_avg_{nh_distance}m ({id});  
    '''.format(nh_distance = nh_distance, id = points_id.lower())
    curs.execute(sql)
    conn.commit()
    print("Done")


print(" Get average NAPLAN score across grades and categories for closest school within 3200m of address... "),
sql = '''
CREATE TABLE IF NOT EXISTS ind_school_naplan_cl_3200m AS
SELECT p.{id},  
       max(naplan."sum"/nullif(naplan.non_null_count::float,0)) AS closest_school_naplan_average,
       o.distance
FROM sample_point_feature p 
LEFT JOIN  
    -- get the distanc and id for closest AOS with school within 3200 m
    (SELECT DISTINCT ON ({id})
            {id}, 
            (obj->>'aos_id')::int AS aos_id, 
            (obj->>'distance')::int AS distance
    FROM od_aos_jsonb, 
        jsonb_array_elements(attributes) obj
    ORDER BY {id}, distance) o  
ON p.{id} = o.{id} 
LEFT JOIN aos_acara_naplan naplan ON o.aos_id = naplan.aos_id 
WHERE naplan.acara_school_id IS NOT NULL 
GROUP BY p.{id}, o.distance;
-- create index 
CREATE UNIQUE INDEX IF NOT EXISTS ind_school_naplan_cl_3200m_idx ON  ind_school_naplan_cl_3200m ({id}); 
'''.format(id = points_id.lower())
curs.execute(sql)
conn.commit()
print("Done")

print("Import ACEQUA database with average ratings and area linkage codes... "),
command = (
          ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
          ' PG:"host={host} port=5432 dbname={db}'
          ' user={user} password = {pwd}" '
          ' {gpkg} "{feature}" '
          ' -nln {name} '
          ' -lco geometry_name="geom" '
          ' -lco  FID=acara_school_id ' .format(host = db_host,
                                                db   = db,
                                                user = db_user,
                                                pwd  = db_pwd,
                                                gpkg = os.path.join(folderPath,childcare_ratings),
                                                feature =  childcare_table,
                                                name    =  childcare_table) 
          )
# print(command)
sp.call(command, shell=True)     

# print("Create ISO37120 indicator (hard threshold is native version; soft threshold is novel...")
# to do... could base on the nh_inds with specific thresholds

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
