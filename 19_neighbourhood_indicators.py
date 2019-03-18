# Script:  16_neighbourhood_indicators.py
# Purpose: Create neighbourhood indicator tables; in particular, daily living and walkability
# Author:  Carl Higgs 
# Date:    20180712

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

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

print("Re-import areas to ensure proper indexing, and restrict other imported areas to study region.")
# Check if the table main_mb_2016_aust_full exists; if it does, these areas have previously been re-imported, so no need to re-do
curs.execute('''SELECT 1 WHERE to_regclass('public.main_mb_2016_aust_full') IS NOT NULL;''')
res = curs.fetchone()
if res is None:
  for area in areas:
    print('{}: '.format(areas[area]['name_f'])), 
    command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" -a_srs "EPSG:{srid}" '.format(srid = srid) \
              + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
              + 'user={user} password = {pwd}" '.format(user = db_user,pwd = db_pwd) \
              + '{shp} '.format(shp = areas[area]['data']) \
              + '-lco geometry_name="geom"  -lco precision=NO ' \
              + '-nlt MULTIPOLYGON' 
    # print(command)
    sp.call(command, shell=True) 
    curs.execute('''
    DELETE FROM  {area} a 
          USING {buffered_study_region} b 
      WHERE NOT ST_Intersects(a.geom,b.geom) 
             OR a.geom IS NULL;
    '''.format(area = areas[area]['table'],
               buffered_study_region = buffered_study_region))
    conn.commit()
else:
  print('''It appears that area tables have previously been imported; nice one.\n''')

print("Create area level destination counts... ")
# We drop these tables first, since some destinations may have been processed since previously running.
# These queries are quick to run, so not much cost to drop and create again.
for area in areas:
  area_name = areas[area]['name_s']
  print("{}... ".format(areas[area]['name_f'])),
  query = '''
  DROP TABLE IF EXISTS {area_name}_dest_counts;
  CREATE TABLE IF NOT EXISTS {area_name}_dest_counts AS
  SELECT a.{area_id}, dest_class, count(d.geom) AS count
  FROM {area_table} a
  LEFT JOIN 
       study_destinations d ON st_contains(a.geom,d.geom)
  GROUP BY a.{area_id},dest_class
  ORDER BY a.{area_id},dest_class;  
  '''.format(area_name = area_name,
             area_table = areas[area]['table'],
             area_id = areas[area]['id'])
  # print(query)
  curs.execute(query)
  conn.commit()
  print("Done.")

# Legacy fallback code: Rename ABS IRSD table if it exists to ensure it works with future scripts
curs.execute('''ALTER TABLE IF EXISTS abs_2016_irsd RENAME TO area_disadvantage;''')
conn.commit()

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

-- a binary threshold indicator  (e.g. of access given distance and threshold)
CREATE OR REPLACE FUNCTION threshold_hard(distance int, threshold int, out int) 
    RETURNS NULL ON NULL INPUT
    AS $$ SELECT (distance < threshold)::int $$
    LANGUAGE SQL;

-- a soft threshold indicator (e.g. of access given distance and threshold)
CREATE OR REPLACE FUNCTION threshold_soft(distance int, threshold int) returns double precision AS 
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
print('Done.')

# Define a series of neighbourhood indicator queries
sql = ['''
-- Daily living
-- DROP TABLE IF EXISTS ind_daily_living;
CREATE TABLE IF NOT EXISTS ind_daily_living AS
SELECT p.{id}, 
       convenience_orig.ind_hard+ supermarket_orig.ind_hard + any_pt.ind_hard AS dl_orig_hard,
       convenience_orig.ind_soft+ supermarket_orig.ind_soft + any_pt.ind_soft AS dl_orig_soft,
       convenience_osm.ind_hard + supermarket_osm.ind_hard + any_pt.ind_hard AS dl_osm_hard,
       convenience_osm.ind_soft + supermarket_osm.ind_soft + any_pt.ind_soft AS dl_osm_soft,  
       GREATEST(convenience_orig.ind_hard, convenience_osm.ind_hard) + GREATEST(supermarket_orig.ind_hard,supermarket_osm.ind_hard) + any_pt.ind_hard AS dl_hyb_hard,
       GREATEST(convenience_orig.ind_soft, convenience_osm.ind_soft) + GREATEST(supermarket_orig.ind_soft,supermarket_osm.ind_soft) + any_pt.ind_soft AS dl_hyb_soft         
FROM parcel_dwellings p  
LEFT JOIN (SELECT {id}, 
                  (CASE WHEN SUM(COALESCE(ind_hard,0)) > 0 THEN 1 ELSE 0 END) AS ind_hard,
                  MAX(COALESCE(ind_soft,0)) AS ind_soft
          FROM od_closest WHERE dest_class IN ('convenience','newsagent','petrolstation') GROUP BY {id}) convenience_orig ON p.{id} = convenience_orig.{id}
LEFT JOIN (SELECT {id}, 
                  (CASE WHEN SUM(COALESCE(ind_hard,0)) > 0 THEN 1 ELSE 0 END) AS ind_hard,
                  MAX(COALESCE(ind_soft,0)) AS ind_soft
          FROM od_closest WHERE dest_class IN ('convenience_osm','newsagent_osm','petrolstation_osm') GROUP BY {id}) convenience_osm ON p.{id} = convenience_osm.{id}
LEFT JOIN (SELECT {id}, ind_hard, ind_soft
           FROM od_closest WHERE dest_class = 'supermarket') supermarket_orig ON p.{id} = supermarket_orig.{id}
LEFT JOIN (SELECT {id}, ind_hard, ind_soft
           FROM od_closest WHERE dest_class = 'supermarket_osm') supermarket_osm ON p.{id} = supermarket_osm.{id}
LEFT JOIN (SELECT {id}, ind_hard, ind_soft 
           FROM od_closest WHERE dest_class = 'gtfs_2018_stops') any_pt ON p.{id} = any_pt.{id};
'''.format(id = points_id),
'''
DROP TABLE ind_local_living_hard;
CREATE TABLE ind_local_living_hard AS
SELECT {0}, (COALESCE(communitycentre_1000m, 0) + 
             COALESCE(libraries_2014_1000m, 0) + 
			 (CASE WHEN COALESCE(childcareoutofschool_1600m, 0) + 
                        COALESCE(childcare_800m, 0) > 0 THEN 1
			  ELSE 0 END) + 
             COALESCE(dentists_1000m, 0) + 
             COALESCE(gp_clinics_1000m, 0) + 
             COALESCE(supermarkets_1000m, 0) + 
             (CASE WHEN COALESCE(conveniencestores_1000m, 0) + 
                        COALESCE(petrolstations_1000m, 0) + 
                        COALESCE(newsagents_1000m, 0) > 0 THEN 1
			  ELSE 0 END) +  
             (CASE WHEN COALESCE(fishmeatpoultryshops_1600m, 0) + 
                        COALESCE(fruitvegeshops_1600m, 0) > 0 THEN 1
			  ELSE 0 END) + 
             COALESCE(pharmacy_1000m, 0) + 
             COALESCE(postoffice_1600m, 0) + 
             COALESCE(banksfinance_1600m, 0) + 
             (CASE WHEN COALESCE(busstop2012_400m, 0) + 
                        COALESCE(tramstops2012_600m, 0) + 
                        COALESCE(trainstations2012_800m,0) > 0 THEN 1
			  ELSE 0 END)) AS local_living 
FROM parcel_dwellings p  
LEFT JOIN (SELECT {id}, 
                  (CASE WHEN SUM(COALESCE(ind_hard,0)) > 0 THEN 1 ELSE 0 END) AS ind_hard,
                  MAX(COALESCE(ind_soft,0)) AS ind_soft
          FROM od_closest WHERE dest_class IN ('community_centre_osm','place_of_worship_osm') GROUP BY {id}) community ON p.{id} = community.{id}
LEFT JOIN (SELECT {id}, 
                  (CASE WHEN SUM(COALESCE(ind_hard,0)) > 0 THEN 1 ELSE 0 END) AS ind_hard,
                  MAX(COALESCE(ind_soft,0)) AS ind_soft
          FROM od_closest WHERE dest_class IN ('convenience_osm','newsagent_osm','petrolstation_osm') GROUP BY {id}) convenience ON p.{id} = convenience.{id}
LEFT JOIN (SELECT {id}, 
                  (CASE WHEN SUM(COALESCE(ind_hard,0)) > 0 THEN 1 ELSE 0 END) AS ind_hard,
                  MAX(COALESCE(ind_soft,0)) AS ind_soft
          FROM od_closest WHERE dest_class IN ('bakery_osm','meat_seafood_osm','fruit_veg_osm','deli_osm') GROUP BY {id}) specialty_food ON p.{id} = specialty_food.{id}
LEFT JOIN (SELECT {id}, 
                  (CASE WHEN SUM(COALESCE(ind_hard,0)) > 0 THEN 1 ELSE 0 END) AS ind_hard,
                  MAX(COALESCE(ind_soft,0)) AS ind_soft
          FROM od_closest WHERE dest_class IN ('childcare_all_meet','childcare_oshc_meet') GROUP BY {id}) childcare ON p.{id} = childcare.{id}
LEFT JOIN (SELECT {id}, ind_hard, ind_soft FROM od_closest WHERE dest_class = 'libraries') libraries ON p.{id} = libraries.{id}
LEFT JOIN (SELECT {id}, ind_hard, ind_soft FROM od_closest WHERE dest_class = 'libraries') libraries ON p.{id} = libraries.{id}
LEFT JOIN (SELECT {id}, ind_hard, ind_soft FROM od_closest WHERE dest_class = 'CommunityHealthCare_Pharmacy') pharmacy ON p.{id} = pharmacy.{id}
LEFT JOIN (SELECT {id}, ind_hard, ind_soft FROM od_closest WHERE dest_class = 'supermarket_osm') supermarket_osm ON p.{id} = supermarket_osm.{id}
LEFT JOIN (SELECT {id}, ind_hard, ind_soft FROM od_closest WHERE dest_class = 'gtfs_2018_stops') any_pt ON p.{id} = any_pt.{id};
''',
'''
DROP TABLE ind_local_living_soft;
CREATE TABLE ind_local_living_soft AS
SELECT {0}, (COALESCE(communitycentre_1000m, 0) + 
             COALESCE(libraries_2014_1000m, 0) + 
			 GREATEST(COALESCE(childcareoutofschool_1600m,0), COALESCE(childcare_800m, 0), COALESCE(dentists_1000m, 0)) + 
             COALESCE(gp_clinics_1000m, 0) + 
             COALESCE(supermarkets_1000m, 0) + 
             GREATEST(COALESCE(conveniencestores_1000m, 0),COALESCE(petrolstations_1000m, 0),COALESCE(newsagents_1000m, 0))+
			 GREATEST(COALESCE(fishmeatpoultryshops_1600m, 0) + 
                        COALESCE(fruitvegeshops_1600m, 0)) +
             COALESCE(pharmacy_1000m, 0) + 
             COALESCE(postoffice_1600m, 0) + 
             COALESCE(banksfinance_1600m, 0) + 
             GREATEST(COALESCE(busstop2012_400m, 0) + 
                        COALESCE(tramstops2012_600m, 0) + 
                        COALESCE(trainstations2012_800m,0))) AS local_living
FROM ind_dest_soft;
''',
'''
-- Walkability
-- DROP TABLE IF EXISTS ind_walkability;
CREATE TABLE IF NOT EXISTS ind_walkability AS
SELECT dl.{id}, 
       z_dl_hard, 
       z_dl_soft, 
       z_sc,
       z_dd,
       z_dl_hard + z_sc + z_dd AS wa_hard,
       z_dl_soft + z_sc + z_dd AS wa_soft
FROM (SELECT {id},
             (dl_hyb_hard - AVG(dl_hyb_hard) OVER())/stddev_pop(dl_hyb_hard) OVER() as z_dl_hard,
             (dl_hyb_soft - AVG(dl_hyb_soft) OVER())/stddev_pop(dl_hyb_soft) OVER() as z_dl_soft FROM ind_daily_living) dl
LEFT JOIN
    (SELECT {id}, (sc_nh1600m - AVG(sc_nh1600m) OVER())/stddev_pop(sc_nh1600m) OVER() as z_sc FROM sc_nh1600m) sc
  ON sc.{id} = dl.{id}
LEFT JOIN
    (SELECT {id}, (dd_nh1600m - AVG(dd_nh1600m) OVER())/stddev_pop(dd_nh1600m) OVER() as z_dd FROM dd_nh1600m) AS dd
  ON dd.{id} = dl.{id};
'''.format(id = points_id),
'''
-- Activity centre proximity
-- DROP TABLE IF EXISTS ind_activity;
CREATE TABLE IF NOT EXISTS ind_activity AS
SELECT {id},
       distance, 
       threshold_hard(distance,1000) AS ind_hard, 
       threshold_soft(distance,1000) AS ind_soft
 FROM od_closest 
WHERE dest_class = 'activity_centres';
'''.format(id = points_id),
'''
-- Supermarkets (1000m)
DROP TABLE IF EXISTS ind_supermarket1000;
CREATE TABLE ind_supermarket1000 AS
SELECT {id}, 
       array_agg(distance) AS distance_array,
       MIN(distance) AS distance,
       threshold_hard(MIN(distance),1000) AS ind_hard, 
       threshold_soft(MIN(distance),1000) AS ind_soft
FROM od_closest WHERE dest_class IN ('supermarket','supermarket_osm')
GROUP BY {id};
'''.format(id = points_id),
'''
-- Food ratio / proportion measures
-- DROP TABLE IF EXISTS ind_foodratio;
CREATE TABLE IF NOT EXISTS ind_foodratio AS
SELECT p.{id}, 
       COALESCE(supermarkets.count,0) AS supermarkets,
       COALESCE(fastfood.count,0) AS fastfood
       -- ,
       -- Commented out in order to speed things up
       --
       --(CASE
       -- WHEN COALESCE(supermarkets.count,0) + COALESCE(fastfood.count,0) !=0 THEN  
       --   COALESCE(supermarkets.count,0)/(COALESCE(supermarkets.count,0) + COALESCE(fastfood.count,0))::float
       -- ELSE NULL END) AS supermarket_proportion 
FROM parcel_dwellings p
LEFT JOIN od_counts AS supermarkets ON p.{id} = supermarkets.{id}
LEFT JOIN od_counts AS fastfood ON p.{id} = fastfood.{id}
  WHERE supermarkets.dest_class = 'supermarket_osm'
    AND fastfood.dest_class = 'fastfood_osm';
'''.format(id = points_id),
'''
-- Public transport proximity
-- DROP TABLE IF EXISTS ind_transport;
CREATE TABLE IF NOT EXISTS ind_transport AS
SELECT p.{id},
       freq30.distance   AS freq30, 
       any_pt.distance   AS any_pt, 
       bus.distance   AS bus, 
       tram.distance  AS tram,
       train.distance AS train,
       ferry.distance AS ferry
FROM parcel_dwellings p
LEFT JOIN (SELECT {id},distance 
             FROM od_closest 
            WHERE dest_class = 'gtfs_2018_stop_30_mins_final') freq30 
       ON p.{id} = freq30.{id} 
LEFT JOIN (SELECT {id},distance 
             FROM od_closest 
            WHERE dest_class = 'gtfs_2018_stops') any_pt 
       ON p.{id} = any_pt.{id} 
LEFT JOIN (SELECT {id},distance 
             FROM od_closest 
            WHERE dest_class = 'gtfs_2018_stops_bus') bus 
       ON p.{id} = bus.{id} 
LEFT JOIN (SELECT {id},distance 
             FROM od_closest 
            WHERE dest_class = 'gtfs_2018_stops_tram') tram 
       ON p.{id} = tram.{id} 
LEFT JOIN (SELECT {id},distance 
             FROM od_closest 
            WHERE dest_class = 'gtfs_2018_stops_train') train 
       ON p.{id} = train.{id} 
LEFT JOIN (SELECT {id},distance 
             FROM od_closest 
            WHERE dest_class = 'gtfs_2018_stops_ferry') ferry 
       ON p.{id} = ferry.{id}
'''.format(id = points_id)  ,
'''
-- Public open space proximity
-- DROP TABLE IF EXISTS ind_pos_closest;
CREATE TABLE IF NOT EXISTS ind_pos_closest AS
SELECT p.{id},
       pos_any.distance   AS pos_any_distance_m, 
       pos_large.distance   AS pos_15k_sqm_distance_m
FROM parcel_dwellings p
LEFT JOIN (SELECT p.{id}, 
                  min(distance) AS distance
             FROM parcel_dwellings p
             LEFT JOIN 
             (SELECT {id},
                    (obj->>'aos_id')::int AS aos_id,
                    (obj->>'distance')::int AS distance
              FROM od_aos_jsonb,
                   jsonb_array_elements(attributes) obj) o ON p.{id} = o.{id}
             LEFT JOIN open_space_areas pos ON o.aos_id = pos.aos_id
                 WHERE pos.aos_id IS NOT NULL
                   AND aos_ha_public > 0
             GROUP BY p.{id}) pos_any ON p.{id} = pos_any.{id}
LEFT JOIN (SELECT p.{id}, 
                  min(distance) AS distance
             FROM parcel_dwellings p
             LEFT JOIN 
             (SELECT {id},
                    (obj->>'aos_id')::int AS aos_id,
                    (obj->>'distance')::int AS distance
              FROM od_aos_jsonb,
                   jsonb_array_elements(attributes) obj) o ON p.{id} = o.{id}
             LEFT JOIN open_space_areas pos ON o.aos_id = pos.aos_id
                 WHERE pos.aos_id IS NOT NULL
                   AND aos_ha_public > 1.5
             GROUP BY p.{id}) pos_large ON p.{id} = pos_large.{id}
'''.format(id = points_id),
'''
-- Create table indexing sport use within 3200m
-- Query these results like: SELECT gnaf_pid, count_in_threshold(distances,1600) FROM sport_3200m;
-- DROP TABLE IF EXISTS sport_3200m;
CREATE TABLE IF NOT EXISTS sport_3200m AS
SELECT p.{id}, 
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
GROUP BY p.{id};
'''.format(id = points_id)
]

for query in sql:
  print('{}... '.format(query.splitlines()[1])),
  curs.execute(query)
  conn.commit()
  print("Done.")

conn.close()

# output to completion log    
script_running_log(script, task, start, locale)
