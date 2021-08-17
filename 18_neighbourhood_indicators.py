# Script:  16_neighbourhood_indicators.py
# Purpose: Compile destinations results and neighbourhood indicator tables
# Author:  Carl Higgs 
# Date:    20190412

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
task = 'create destination indicator tables'

schema = 'ind_point'

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

# initial postgresql connection
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db), 
                       use_native_hstore=False)
                       
print("Create area level destination counts... ")
# We drop these tables first, since some destinations may have been processed since previously running.
# These queries are quick to run, so not much cost to drop and create again.
for area in analysis_regions:
  area_id = df_regions.loc[area,'id']
  abbrev = df_regions.loc[area,'abbreviation']
  print("{}... ".format(area)),
  query = '''
  --DROP TABLE IF EXISTS ind_{abbrev}.{abbrev}_dest_counts;
  CREATE TABLE IF NOT EXISTS ind_{abbrev}.{abbrev}_dest_counts AS
  SELECT a.{area_id}, destination, count(d.geom) AS count
  FROM area_linkage a
  LEFT JOIN 
       destinations.study_destinations d ON st_contains(a.geom,d.geom)
  GROUP BY a.{area_id},destination
  ORDER BY a.{area_id},destination;  
  '''.format(abbrev = abbrev,
             area_id = area_id)
  # print(query)
  engine.execute(query)
  print("Done.")

# Get a list of all potential destinations for distance to closest 
# (some may not be present in region, will be null, so we can refer to them in later queries)
# destination names
categories = sorted(set([x for x in df_destinations.destination.tolist()]))
category_list = ','.join(categories)
category_types = '"{}" int'.format('" int, "'.join(categories))

array_category_types = '"{}" int[]'.format('" int[], "'.join(categories))

# Neighbourhood_indicators
print("Create nh_inds_distance (curated distance to closest table for re-use by other indicators)... "),
table = 'nh_inds_distance'
sql = '''
DROP TABLE IF EXISTS {schema}.{table};
CREATE TABLE IF NOT EXISTS {schema}.{table} AS
SELECT 
       p.{points_id},
       LEAST(array_min(convenience_osm.distances),
             array_min(newsagent_osm.distances),
             array_min(petrolstation_osm.distances),
             array_min(market_osm.distances)) AS convenience_osm_2018,
       LEAST(array_min(supermarkets_2017.distances),
             array_min(supermarket_osm.distances)) AS supermarket_hlc_2017_osm_2018,
       LEAST(array_min(community_centre_osm.distances),
             array_min(hlc_2016_community_centres.distances)) AS community_centre_hlc_2016_osm_2018,
       LEAST(array_min(bakery_osm.distances),
             array_min(meat_seafood_osm.distances),
             array_min(fruit_veg_osm.distances),
             array_min(deli_osm.distances)) AS food_fresh_specialty_osm_2018,
       LEAST(array_min(fastfood_osm.distances),
             array_min(food_court_osm.distances),
             array_min(fastfood_2017.distances)) AS food_fast_hlc_2017_osm_2018,         
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
       LEAST(array_min("p_12_schools_gov_2018".distances), 
             array_min(primary_schools_gov_2018.distances)) AS schools_primary_all_gov,           
       LEAST(array_min("p_12_schools_gov_2018".distances), 
             array_min(secondary_schools_gov_2018.distances)) AS schools_secondary_all_gov,
       ind_pt_d_800m_cl_headway_day_2019_oct8_dec5_0700_1900.pt_any AS gtfs_20191008_20191205_pt_any,
       ind_pt_d_800m_cl_headway_day_2019_oct8_dec5_0700_1900.pt_h20min AS gtfs_20191008_20191205_pt_0020
FROM {sample_point_feature} p
LEFT JOIN d_3200m_cl."p_12_schools_gov_2018"   ON p.{points_id}    = d_3200m_cl."p_12_schools_gov_2018".{points_id}
LEFT JOIN d_3200m_cl.primary_schools_gov_2018  ON p.{points_id}    = d_3200m_cl.primary_schools_gov_2018.{points_id}
LEFT JOIN d_3200m_cl.secondary_schools_gov_2018  ON p.{points_id}    = d_3200m_cl.secondary_schools_gov_2018.{points_id}
LEFT JOIN ind_point.ind_pt_d_800m_cl_headway_day_2019_oct8_dec5_0700_1900   ON p.{points_id}    = ind_point.ind_pt_d_800m_cl_headway_day_2019_oct8_dec5_0700_1900.{points_id}
LEFT JOIN d_3200m_cl.convenience_osm   ON p.{points_id}    = d_3200m_cl.convenience_osm.{points_id}
LEFT JOIN d_3200m_cl.newsagent_osm     ON p.{points_id}    = d_3200m_cl.newsagent_osm.{points_id}
LEFT JOIN d_3200m_cl.petrolstation_osm ON p.{points_id}    = d_3200m_cl.petrolstation_osm.{points_id}
LEFT JOIN d_3200m_cl.market_osm        ON p.{points_id}    = d_3200m_cl.market_osm.{points_id}
LEFT JOIN d_3200m_cl.supermarkets_2017 ON p.{points_id}    = d_3200m_cl.supermarkets_2017.{points_id}
LEFT JOIN d_3200m_cl.supermarket_osm ON p.{points_id} = d_3200m_cl.supermarket_osm.{points_id} 
LEFT JOIN d_3200m_cl.community_centre_osm ON p.{points_id} = d_3200m_cl.community_centre_osm.{points_id}
LEFT JOIN d_3200m_cl.hlc_2016_community_centres ON p.{points_id} = d_3200m_cl.hlc_2016_community_centres.{points_id}
LEFT JOIN d_3200m_cl.bakery_osm       ON p.{points_id} = d_3200m_cl.bakery_osm.{points_id}
LEFT JOIN d_3200m_cl.meat_seafood_osm ON p.{points_id} = d_3200m_cl.meat_seafood_osm.{points_id}
LEFT JOIN d_3200m_cl.fruit_veg_osm   ON p.{points_id}  = d_3200m_cl.fruit_veg_osm.{points_id}
LEFT JOIN d_3200m_cl.deli_osm        ON p.{points_id}  = d_3200m_cl.deli_osm.{points_id}
LEFT JOIN d_3200m_cl.fastfood_osm    ON p.{points_id}  = d_3200m_cl.fastfood_osm.{points_id}
LEFT JOIN d_3200m_cl.food_court_osm  ON p.{points_id}  = d_3200m_cl.food_court_osm.{points_id}            
LEFT JOIN d_3200m_cl.fastfood_2017   ON p.{points_id}  = d_3200m_cl.fastfood_2017.{points_id}
LEFT JOIN d_3200m_cl.restaurant_osm  ON p.{points_id}  = d_3200m_cl.restaurant_osm.{points_id}
LEFT JOIN d_3200m_cl.cafe_osm        ON p.{points_id}  = d_3200m_cl.cafe_osm.{points_id}
LEFT JOIN d_3200m_cl.museum_osm      ON p.{points_id}  = d_3200m_cl.museum_osm.{points_id} 
LEFT JOIN d_3200m_cl.theatre_osm     ON p.{points_id}  = d_3200m_cl.theatre_osm.{points_id} 
LEFT JOIN d_3200m_cl.cinema_osm      ON p.{points_id}  = d_3200m_cl.cinema_osm.{points_id} 
LEFT JOIN d_3200m_cl.art_gallery_osm ON p.{points_id}  = d_3200m_cl.art_gallery_osm.{points_id} 
LEFT JOIN d_3200m_cl.art_centre_osm  ON p.{points_id}  = d_3200m_cl.art_centre_osm.{points_id} 
LEFT JOIN d_3200m_cl.bar_osm         ON p.{points_id}  = d_3200m_cl.bar_osm.{points_id} 
LEFT JOIN d_3200m_cl.nightclub_osm   ON p.{points_id}  = d_3200m_cl.nightclub_osm.{points_id}
LEFT JOIN d_3200m_cl.pub_osm         ON p.{points_id}  = d_3200m_cl.pub_osm.{points_id}  
;
CREATE UNIQUE INDEX IF NOT EXISTS {table}_idx ON  {schema}.{table} ({points_id}); 
'''.format(sample_point_feature = sample_point_feature, schema = schema, points_id = points_id,table = table)
# print(sql)
curs.execute(sql)
conn.commit()
print("Done.")
  
print("Create hard and soft threshold indicators for curated destination categories...")
for threshold_type in ['hard','soft']:
    for nh_threshold in [400,800,1000,1600]:
        print("  - nh_inds_{threshold_type}_{nh_threshold}m".format(threshold_type = threshold_type, nh_threshold = nh_threshold))
        sql = '''
        DROP TABLE IF EXISTS {schema}.nh_inds_{threshold_type}_{nh_threshold}m;
        CREATE TABLE IF NOT EXISTS {schema}.nh_inds_{threshold_type}_{nh_threshold}m AS
        SELECT  
        {points_id},
        threshold_{threshold_type}(convenience_osm_2018,{nh_threshold}) AS convenience_osm_2018, 
        threshold_{threshold_type}(supermarket_hlc_2017_osm_2018,{nh_threshold}) AS supermarket_hlc_2017_osm_2018, 
        threshold_{threshold_type}(community_centre_hlc_2016_osm_2018,{nh_threshold}) AS community_centre_hlc_2016_osm_2018,
        threshold_{threshold_type}(food_fresh_specialty_osm_2018,{nh_threshold}) AS food_fresh_specialty_osm_2018 ,
        threshold_{threshold_type}(food_fast_hlc_2017_osm_2018,{nh_threshold}) AS food_fast_hlc_2017_osm_2018,
        threshold_{threshold_type}(food_dining_osm_2018,{nh_threshold}) AS food_dining_osm_2018,
        threshold_{threshold_type}(culture_osm_2018,{nh_threshold}) AS culture_osm_2018,
        threshold_{threshold_type}(gtfs_20191008_20191205_pt_any    ,{nh_threshold}) AS gtfs_20191008_20191205_pt_any,
        threshold_{threshold_type}(gtfs_20191008_20191205_pt_0020    ,{nh_threshold}) AS gtfs_20191008_20191205_pt_0020
        FROM {schema}.nh_inds_distance ;
        CREATE UNIQUE INDEX IF NOT EXISTS nh_inds_{threshold_type}_{nh_threshold}m_idx ON  {schema}.nh_inds_{threshold_type}_{nh_threshold}m ({points_id}); 
        '''.format(points_id = points_id,schema = schema, threshold_type = threshold_type, nh_threshold = nh_threshold)
        engine.execute(sql)
        
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
        from_list +=['LEFT JOIN {schema}.nh_inds_{t}_{d}m ON p.{points_id} = {schema}.nh_inds_{t}_{d}m.{points_id}'.format(t=t,d=d,schema = schema, points_id=points_id)]
sql = '''
DROP TABLE IF EXISTS {schema}.{table};
CREATE TABLE {schema}.{table} AS
SELECT p.{points_id},
       {inds}
FROM {sample_point_feature} p
     {from_list};
CREATE UNIQUE INDEX {table}_idx ON  {schema}.{table} ({points_id});
'''.format(inds=','.join(ind_list), 
           schema = schema, 
           sample_point_feature=sample_point_feature,
           from_list = '\r\n'.join(from_list),
           points_id = points_id,
           table = table)
engine.execute(sql)

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
            COALESCE(threshold_{t}(array_min(d_3200m_cl.libraries_2018.distances),{d}),0) +
            COALESCE(threshold_{t}(array_min(d_3200m_cl.childcare_all_meet_2019.distances),{d}),0) +
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
        from_list +=['LEFT JOIN {schema}.nh_inds_{t}_{d}m ON p.{points_id} = {schema}.nh_inds_{t}_{d}m.{points_id}'.format(t=t,d=d,
           schema = schema, points_id=points_id)]
sql = '''
DROP TABLE IF EXISTS {schema}.{table};
CREATE TABLE {schema}.{table} AS
SELECT p.{points_id},
       {inds}
FROM {sample_point_feature} p
     {from_list}
    LEFT JOIN d_3200m_cl.libraries_2018 ON p.{points_id} = d_3200m_cl.libraries_2018.{points_id}
    LEFT JOIN d_3200m_cl.childcare_all_meet_2019 ON p.{points_id} = d_3200m_cl.childcare_all_meet_2019.{points_id}
    LEFT JOIN d_3200m_cl. nhsd_2017_dentist ON p.{points_id} = d_3200m_cl. nhsd_2017_dentist.{points_id}
    LEFT JOIN d_3200m_cl. nhsd_2017_gp ON p.{points_id} = d_3200m_cl. nhsd_2017_gp.{points_id}
    LEFT JOIN d_3200m_cl. nhsd_2017_pharmacy ON p.{points_id} = d_3200m_cl. nhsd_2017_pharmacy.{points_id}
    LEFT JOIN d_3200m_cl.postoffice_osm ON p.{points_id} = d_3200m_cl.postoffice_osm.{points_id} ;
CREATE UNIQUE INDEX {table}_idx ON  {schema}.{table} ({points_id});
'''.format(inds=','.join(ind_list), 
           sample_point_feature=sample_point_feature,
           schema = schema, 
           from_list = '\r\n'.join(from_list),
           points_id = points_id,
           table = table)
engine.execute(sql)

print(" Done.")

# calculate food indicators at both 1600 m and 3200 m 
print(" - ind_food... "),
res = engine.has_table('ind_food',schema=schema)
if res:
    print("Table exists.")
else:
    for nh_threshold in [1600,3200]:
        table = ['ind_food_{nh_threshold}m'.format(nh_threshold = nh_threshold),'f']
        sql = '''
        DROP TABLE IF EXISTS {schema}.{table};
        CREATE TABLE IF NOT EXISTS {schema}.{table} AS
        SELECT
            {points_id},
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
                p.{points_id},
                GREATEST(COALESCE(count_in_threshold(supermarkets_2017.distances,{nh_threshold}),0),
                        COALESCE(count_in_threshold(supermarket_osm.distances,{nh_threshold}),0)) AS supermarkets,
                COALESCE(count_in_threshold(fruit_veg_osm.distances,{nh_threshold}),0) AS fruit_veg,
                (COALESCE(count_in_threshold(bakery_osm.distances,{nh_threshold}),0) +       
                COALESCE(count_in_threshold(meat_seafood_osm.distances,{nh_threshold}),0) +          
                COALESCE(count_in_threshold(deli_osm.distances,{nh_threshold}),0)) AS specialty,         
                GREATEST(COALESCE(count_in_threshold(fastfood_2017.distances,{nh_threshold}),0),
                        COALESCE(count_in_threshold(fastfood_osm.distances,{nh_threshold}),0)) AS fastfood
            FROM {sample_point_feature} p
            LEFT JOIN d_3200m_cl.supermarkets_2017 ON p.{points_id} = d_3200m_cl.supermarkets_2017.{points_id}
            LEFT JOIN d_3200m_cl.supermarket_osm ON p.{points_id} = d_3200m_cl.supermarket_osm.{points_id}
            LEFT JOIN d_3200m_cl.fruit_veg_osm ON p.{points_id} = d_3200m_cl.fruit_veg_osm.{points_id}
            LEFT JOIN d_3200m_cl.bakery_osm ON p.{points_id} = d_3200m_cl.bakery_osm.{points_id}
            LEFT JOIN d_3200m_cl.meat_seafood_osm ON p.{points_id} = d_3200m_cl.meat_seafood_osm.{points_id}
            LEFT JOIN d_3200m_cl.deli_osm ON p.{points_id} = d_3200m_cl.deli_osm.{points_id}
            LEFT JOIN d_3200m_cl.fastfood_2017 ON p.{points_id} = d_3200m_cl.fastfood_2017.{points_id}
            LEFT JOIN d_3200m_cl.fastfood_osm ON p.{points_id} = d_3200m_cl.fastfood_osm.{points_id}
            )d
        '''.format(table = table[0], 
                schema = schema, 
                points_id = points_id,
                sample_point_feature=sample_point_feature,
                nh_threshold = nh_threshold)
        engine.execute(sql)
        create_index = '''CREATE UNIQUE INDEX IF NOT EXISTS {table}_idx ON  {schema}.{table} ({points_id});  '''.format(table = table[0],schema=schema,points_id = points_id)
        engine.execute(create_index)
        
    
    # combine food tables
    sql = '''
    DROP TABLE IF EXISTS {schema}.ind_food;
    CREATE TABLE IF NOT EXISTS {schema}.ind_food AS
    SELECT * FROM {schema}.ind_food_1600m LEFT JOIN {schema}.ind_food_3200m USING ({points_id});
    CREATE UNIQUE INDEX IF NOT EXISTS ind_food_idx ON  {schema}.ind_food ({points_id});
    DROP TABLE IF EXISTS {schema}.ind_food_1600m;
    DROP TABLE IF EXISTS {schema}.ind_food_3200m;
    '''.format(points_id = points_id,
           schema = schema)
    engine.execute(sql)
    print(" Done.")

# Create Open Space measures (distances, which can later be considered with regard to thresholds)
# In addition to public open space (pos), also includes sport areas and blue space
table = ['ind_os_distances_3200m','os']
print(" - {schema}.{table}".format(table = table[0],schema=schema)),

sql = '''
DROP TABLE IF EXISTS {schema}.{table};
CREATE TABLE IF NOT EXISTS {schema}.{table} AS SELECT {points_id} FROM {sample_point_feature};
'''.format(table = table[0], sample_point_feature=sample_point_feature,schema=schema,points_id = points_id)
engine.execute(sql)


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
    SELECT 1
    FROM information_schema.columns 
    WHERE table_name='{table}' AND table_schema='{schema}' AND column_name='{column}';
    '''.format(table = table[0],schema=schema,column = measure)
    res = engine.execute(sql).fetchone()
    if not res:   
        add_and_update_measure = '''
        DROP INDEX IF EXISTS {schema}.{table}_idx;
        ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS {measure} int[];
        UPDATE {schema}.{table} t 
        SET {measure} = os_filtered.distances
        FROM {sample_point_feature} orig
        LEFT JOIN (SELECT p.{points_id}, 
                        array_agg(distance) AS distances
                    FROM {sample_point_feature} p
                    LEFT JOIN 
                    (SELECT {points_id},
                            (obj->>'aos_id')::int AS aos_id,
                            (obj->>'distance')::int AS distance
                    FROM ind_point.od_aos_3200m_cl,
                        jsonb_array_elements(attributes) obj) o ON p.{points_id} = o.{points_id}
                    LEFT JOIN open_space.open_space_areas pos ON o.aos_id = pos.aos_id
                        WHERE pos.aos_id IS NOT NULL
                        AND {where}
                    GROUP BY p.{points_id}) os_filtered ON orig.{points_id} = os_filtered.{points_id}
        WHERE t.{points_id} = orig.{points_id};
        '''.format(points_id = points_id,
                   schema=schema,
                   sample_point_feature=sample_point_feature, 
                   table = table[0], 
                   measure = measure,
                   where = aos[1])
        engine.execute(add_and_update_measure)
        
    print("."),

measure = 'sport_distances_3200m'
sql = '''
SELECT 1 
FROM information_schema.columns 
WHERE table_name='{table}' AND table_schema = '{schema}' AND column_name='{column}';
'''.format(table = table[0],schema=schema,column = measure)
res = engine.execute(sql).fetchone()
if not res:     
    add_and_update_measure = '''
    DROP INDEX IF EXISTS {schema}.{table}_idx;
    ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS {measure} int[];
    UPDATE {schema}.{table} t 
    SET {measure} = os_filtered.distances
    FROM {sample_point_feature} orig
    LEFT JOIN (SELECT p.{points_id}, 
                    array_agg(distance) AS distances
            FROM {sample_point_feature} p
            LEFT JOIN (SELECT {points_id},
                                (obj->>'aos_id')::int AS aos_id,
                                (obj->>'distance')::int AS distance
                            FROM ind_point.od_aos_3200m_cl,
                                jsonb_array_elements(attributes) obj
                        WHERE (obj->>'distance')::int < 3200) o ON p.{points_id} = o.{points_id}                  
            WHERE EXISTS -- we restrict our results to distances to AOS with sports facilities 
                        (SELECT 1 FROM open_space.open_space_areas sport,
                                        jsonb_array_elements(attributes) obj
                            WHERE (obj->>'leisure' IN ('golf_course','sports_club','sports_centre','fitness_centre','pitch','track','fitness_station','ice_rink','swimming_pool') 
                            OR (obj->>'sport' IS NOT NULL 
                            AND obj->>'sport' != 'no'))
                            AND  o.aos_id = sport.aos_id)
            GROUP BY p.{points_id} ) os_filtered ON orig.{points_id} = os_filtered.{points_id}
    WHERE t.{points_id} = orig.{points_id};
    '''.format(points_id = points_id,
           sample_point_feature=sample_point_feature, 
           schema=schema,
           table = table[0], 
           measure = measure)
    engine.execute(add_and_update_measure)


measure = 'pos_toilet_distances_3200m'
sql = '''
SELECT column_name 
FROM information_schema.columns 
WHERE table_name='{table}' AND table_schema = '{schema}' AND column_name='{column}';
'''.format(table = table[0],column = measure,schema=schema)
res = engine.execute(sql).fetchone()
if not res:     
    add_and_update_measure = '''
    DROP INDEX IF EXISTS {table}_idx;
    ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS {measure} int[];
    UPDATE {schema}.{table} t 
    SET {measure} = os_filtered.distances
    FROM {sample_point_feature} orig
    LEFT JOIN (SELECT DISTINCT ON (p.{points_id}) 
                    p.{points_id}, 
                    array_agg(distance) AS distances
            FROM {sample_point_feature} p
            LEFT JOIN   
                        (SELECT {points_id},  
                        (obj->>'aos_id')::int AS aos_id, 
                        (obj->>'distance')::int AS distance 
                        FROM ind_point.od_aos_3200m_cl, 
                        jsonb_array_elements(attributes) obj) o ON p.{points_id} = o.{points_id} 
            LEFT JOIN open_space.open_space_areas pos ON o.aos_id = pos.aos_id 
                WHERE pos.aos_id IS NOT NULL  
                    AND co_location_100m ? 'toilets_2018'
            GROUP BY p.{points_id} ) os_filtered ON orig.{points_id} = os_filtered.{points_id}
    WHERE t.{points_id} = orig.{points_id};
    '''.format(points_id = points_id, 
           sample_point_feature=sample_point_feature,
           schema=schema,
           table = table[0],
           measure = measure)
    engine.execute(add_and_update_measure)
    
print("."),
print(" Done.")

create_index = '''CREATE UNIQUE INDEX IF NOT EXISTS {table}_idx ON  {schema}.{table} ({points_id});  '''.format(table = table[0], schema=schema, points_id = points_id)
engine.execute(create_index)


table = ['ind_os_distance','os']
print(" - {schema}.{table}".format(table = table[0],schema=schema)),    
sql = '''
DROP TABLE IF EXISTS {schema}.ind_os_distance;
CREATE TABLE IF NOT EXISTS {schema}.ind_os_distance AS
SELECT 
    {points_id},
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
FROM {schema}.ind_os_distances_3200m;
'''.format(points_id = points_id,schema=schema)    
engine.execute(sql)

create_index = '''CREATE UNIQUE INDEX IF NOT EXISTS {table}_idx ON  {schema}.{table} ({points_id});  '''.format(table = table[0],schema=schema, points_id = points_id)
engine.execute(create_index)

print("Done.")
    
print("Import ACARA schools look up table with NAPLAN results and area linkage codes... "),
# Check if the table main_mb_2016_aust_full exists; if it does, these areas have previously been re-imported, so no need to re-do
command = (
          ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
          ' PG:"host={host} port=5432 dbname={db} active_schema=schools '
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
DROP TABLE IF EXISTS schools.aos_acara_naplan;
CREATE TABLE IF NOT EXISTS schools.aos_acara_naplan AS 
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
    FROM open_space.open_space_areas schools, 
         jsonb_array_elements(schools.attributes) obj, 
         jsonb_array_elements((obj ->>'school_tags')::jsonb) tags) os_acara 
    -- join schools with their naplan scores 
LEFT JOIN schools.{table} ON os_acara.acara_school_id = schools.{table}.acara_school_id 
WHERE os_acara.acara_school_id IS NOT NULL; 
-- create index 
CREATE UNIQUE INDEX IF NOT EXISTS aos_acara_naplan_idx ON  schools.aos_acara_naplan (aos_id,acara_school_id);  
'''.format(table = school_table)

curs.execute(sql)
conn.commit()

print("Done.")

for nh_distance in [800,1600]:
    print(" Get NAPLAN average of all schools within {}m of address... ".format(nh_distance)),
    sql = '''
    DROP TABLE IF EXISTS {schema}.ind_school_naplan_avg_{nh_distance}m;
    CREATE TABLE IF NOT EXISTS {schema}.ind_school_naplan_avg_{nh_distance}m AS
    SELECT p.{points_id},  
        COUNT(acara_school_id) AS school_count_{nh_distance}m, 
        AVG(o.distance)::int AS average_distance_{nh_distance}m, 
        AVG(sum) AS average_sum_of_naplan_{nh_distance}m, 
        AVG(non_null_count) AS average_test_count_{nh_distance}m, 
        AVG(sum/ nullif(non_null_count::float,0)) AS naplan_average_{nh_distance}m 
    FROM {sample_point_feature} p 
    LEFT JOIN  
        -- get the distances and ids for all AOS with schools within 3200 m
        (SELECT {points_id}, 
                (obj->>'aos_id')::int AS aos_id, 
                (obj->>'distance')::int AS distance 
        FROM ind_point.od_aos_3200m_cl, 
            jsonb_array_elements(attributes) obj) o  
    ON p.{points_id} = o.{points_id} 
    LEFT JOIN schools.aos_acara_naplan naplan ON o.aos_id = naplan.aos_id 
    WHERE naplan.acara_school_id IS NOT NULL 
        AND o.distance < {nh_distance}
    GROUP BY p.{points_id} ;
    -- create index 
    CREATE UNIQUE INDEX IF NOT EXISTS ind_school_naplan_avg_{nh_distance}m_idx
                     ON  {schema}.ind_school_naplan_avg_{nh_distance}m ({points_id});  
    '''.format(nh_distance = nh_distance,
           schema=schema,
           sample_point_feature=sample_point_feature,
           points_id = points_id)
    engine.execute(sql)
    
    print("Done")


print(" Get average NAPLAN score across grades and categories for closest school within 3200m of address... "),
sql = '''
DROP TABLE IF EXISTS {schema}.ind_school_naplan_cl_3200m;
CREATE TABLE IF NOT EXISTS {schema}.ind_school_naplan_cl_3200m AS
SELECT p.{points_id},  
       max(naplan."sum"/nullif(naplan.non_null_count::float,0)) AS closest_school_naplan_average,
       o.distance
FROM {sample_point_feature} p 
LEFT JOIN  
    -- get the distanc and id for closest AOS with school within 3200 m
    (SELECT DISTINCT ON ({points_id})
            {points_id}, 
            (obj->>'aos_id')::int AS aos_id, 
            (obj->>'distance')::int AS distance
    FROM ind_point.od_aos_3200m_cl, 
        jsonb_array_elements(attributes) obj
    ORDER BY {points_id}, distance) o  
ON p.{points_id} = o.{points_id} 
LEFT JOIN schools.aos_acara_naplan naplan ON o.aos_id = naplan.aos_id 
WHERE naplan.acara_school_id IS NOT NULL 
GROUP BY p.{points_id}, o.distance;
-- create index 
CREATE UNIQUE INDEX IF NOT EXISTS ind_school_naplan_cl_3200m_idx ON  {schema}.ind_school_naplan_cl_3200m ({points_id}); 
'''.format(points_id = points_id,
           schema=schema,
           sample_point_feature=sample_point_feature)
engine.execute(sql)

print("Done")

print("Import ACEQUA database with average ratings and area linkage codes... "),
command = (
          ' ogr2ogr -overwrite -progress -f "PostgreSQL"  ' 
          ' PG:"host={host} port=5432 dbname={db} active_schema=schools '
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
engine.dispose()
conn.close()
