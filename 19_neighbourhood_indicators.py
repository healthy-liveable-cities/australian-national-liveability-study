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

create_threshold_functions = '''
CREATE OR REPLACE FUNCTION threshold_hard(in int, in int, out int) 
    RETURNS NULL ON NULL INPUT
    AS $$ SELECT ($1 < $2)::int $$
    LANGUAGE SQL;

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

print('Creating or replacing threshold functions ... '),
curs.execute(create_threshold_functions)
print('Done.')


sql = ['''
-- Daily living
DROP TABLE IF EXISTS ind_daily_living;
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
-- Walkability
DROP TABLE IF EXISTS ind_walkability;
CREATE TABLE ind_walkability AS
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
DROP TABLE IF EXISTS ind_activity;
CREATE TABLE ind_activity AS
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
DROP TABLE IF EXISTS ind_foodratio;
CREATE TABLE ind_foodratio AS
SELECT p.{id}, 
       supermarkets.count AS supermarkets,
       fastfood.count AS fastfood,
       (CASE
        WHEN ((COALESCE(supermarkets.count,0))+(COALESCE(fastfood.count,0))) !=0 THEN  
          (COALESCE(supermarkets.count,0))/((COALESCE(supermarkets.count,0))+(COALESCE(fastfood.count,0)))::double precision
        ELSE NULL END) AS supermarket_proportion 
FROM parcel_dwellings p
LEFT JOIN (SELECT {id},COALESCE(count,0) AS count FROM od_counts WHERE dest_class = 'supermarket_osm') AS supermarkets 
  ON p.{id} = supermarkets.{id}
LEFT JOIN (SELECT {id},COALESCE(count,0) AS count FROM od_counts WHERE dest_class = 'fastfood_osm') AS fastfood 
  ON p.{id} = fastfood.{id};
'''.format(id = points_id),
'''
-- Public transport proximity
DROP TABLE IF EXISTS ind_transport;
CREATE TABLE ind_transport AS
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
DROP TABLE IF EXISTS ind_pos_closest;
CREATE TABLE ind_pos_closest AS
SELECT p.{id},
       pos_any.distance   AS pos_any_distance_m, 
       pos_large.distance   AS pos_15k_sqm_distance_m
FROM parcel_dwellings p
LEFT JOIN (SELECT p.gnaf_pid, 
                  min(distance) AS distance
             FROM parcel_dwellings p
             LEFT JOIN 
             (SELECT gnaf_pid,
                    (obj->>'aos_id')::int AS aos_id,
                    (obj->>'distance')::int AS distance
              FROM od_aos_jsonb,
                   jsonb_array_elements(attributes) obj) o ON p.gnaf_pid = o.gnaf_pid
             LEFT JOIN open_space_areas pos ON o.aos_id = pos.aos_id
                 WHERE pos.aos_id IS NOT NULL
                   AND aos_ha_public > 0
             GROUP BY p.gnaf_pid) pos_any ON p.gnaf_pid = pos_any.gnaf_pid
LEFT JOIN (SELECT p.gnaf_pid, 
                  min(distance) AS distance
             FROM parcel_dwellings p
             LEFT JOIN 
             (SELECT gnaf_pid,
                    (obj->>'aos_id')::int AS aos_id,
                    (obj->>'distance')::int AS distance
              FROM od_aos_jsonb,
                   jsonb_array_elements(attributes) obj) o ON p.gnaf_pid = o.gnaf_pid
             LEFT JOIN open_space_areas pos ON o.aos_id = pos.aos_id
                 WHERE pos.aos_id IS NOT NULL
                   AND aos_ha_public > 1.5
             GROUP BY p.gnaf_pid) pos_large ON p.gnaf_pid = pos_large.gnaf_pid
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
