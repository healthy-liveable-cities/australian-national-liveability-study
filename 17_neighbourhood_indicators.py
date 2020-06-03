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
       array_min(gtfs_20191008_20191205_revised_frequent30.distances) AS gtfs_20191008_20191205_pt_0030,
       array_min(gtfs_20191008_20191205_revised_all.distances) AS gtfs_20191008_20191205_pt_any
FROM parcel_dwellings p
LEFT JOIN d_3200m_cl."P_12_Schools_gov"   ON p.{id}    = d_3200m_cl."P_12_Schools_gov".{id}
LEFT JOIN d_3200m_cl.primary_schools_gov   ON p.{id}    = d_3200m_cl.primary_schools_gov.{id}
LEFT JOIN d_3200m_cl.secondary_schools_gov   ON p.{id}    = d_3200m_cl.secondary_schools_gov.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_revised_frequent30   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_revised_frequent30.{id}
LEFT JOIN d_3200m_cl.gtfs_20191008_20191205_revised_all   ON p.{id}    = d_3200m_cl.gtfs_20191008_20191205_revised_all.{id}
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
        threshold_{threshold_type}(supermarket_hlc_2017_osm_2018,{nh_threshold}) AS supermarket_hlc_2017_osm_2018, 
        threshold_{threshold_type}(community_centre_hlc_2016_osm_2018,{nh_threshold}) AS community_centre_hlc_2016_osm_2018,
        threshold_{threshold_type}(food_fresh_specialty_osm_2018,{nh_threshold}) AS food_fresh_specialty_osm_2018 ,
        threshold_{threshold_type}(food_fast_hlc_2017_osm_2018,{nh_threshold}) AS food_fast_hlc_2017_osm_2018,
        threshold_{threshold_type}(food_dining_osm_2018,{nh_threshold}) AS food_dining_osm_2018,
        threshold_{threshold_type}(culture_osm_2018,{nh_threshold}) AS culture_osm_2018,
        threshold_{threshold_type}(gtfs_20191008_20191205_pt_any    ,{nh_threshold}) AS gtfs_20191008_20191205_pt_any,
        threshold_{threshold_type}(gtfs_20191008_20191205_pt_0030    ,{nh_threshold}) AS gtfs_20191008_20191205_pt_0030
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
           COALESCE(nh_inds_{t}_{d}m.gtfs_20191008_20191205_pt_any,0)) AS {abbrev}_{t}_{d}m
        '''.format(t=t,d=d,abbrev=abbrev)
        ind_list  +=[inds]
        from_list +=['LEFT JOIN nh_inds_{t}_{d}m ON p.{id} = nh_inds_{t}_{d}m.{id}'.format(t=t,d=d,id=points_id)]
sql = '''
DROP TABLE IF EXISTS {table};
CREATE TABLE {table} AS
SELECT p.{id},
       {inds}
FROM parcel_dwellings p
     {from_list};
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
            COALESCE(nh_inds_{t}_{d}m.gtfs_20191008_20191205_pt_any,0)) AS {abbrev}_{t}_{d}m
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
    LEFT JOIN d_3200m_cl.postoffice_osm ON p.{id} = d_3200m_cl.postoffice_osm.{id} ;
CREATE UNIQUE INDEX {table}_idx ON  {table} ({id});
'''.format(inds=','.join(ind_list), 
           from_list = '\r\n'.join(from_list),
           id = points_id,
           table = table)
curs.execute(sql)
conn.commit()
print(" Done.")

# calculate food indicators at both 1600 m and 3200 m 
print(" - ind_food... "),
curs.execute('''SELECT 1 WHERE to_regclass('public.{table}') IS NOT NULL;'''.format(table = 'ind_food'))
res = curs.fetchone()
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
CREATE TABLE IF NOT EXISTS {table} AS SELECT {id} FROM parcel_dwellings;
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
        FROM parcel_dwellings orig
        LEFT JOIN (SELECT p.{id}, 
                        array_agg(distance) AS distances
                    FROM parcel_dwellings p
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
    FROM parcel_dwellings orig
    LEFT JOIN (SELECT p.{id}, 
                    array_agg(distance) AS distances
            FROM parcel_dwellings p
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
    FROM parcel_dwellings orig
    LEFT JOIN (SELECT DISTINCT ON (p.{id}) 
                    p.{id}, 
                    array_agg(distance) AS distances
            FROM parcel_dwellings p
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
    FROM parcel_dwellings p 
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
FROM parcel_dwellings p 
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
