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

CREATE OR REPLACE FUNCTION threshold_soft(in int, in int, out double precision) 
    RETURNS NULL ON NULL INPUT
    AS $$ SELECT 1 - 1/(1+exp(-5*($1-$2)/($2::float))) $$
    LANGUAGE SQL;    
  '''

print('Creating or replacing threshold functions ... '),
curs.execute(create_threshold_functions)
print('Done.')


dl = '''
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
'''.format(id = points_id)

wa= '''
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
'''.format(id = points_id)


activity = '''
DROP TABLE IF EXISTS ind_activity;
CREATE TABLE ind_activity AS
SELECT {id},
       distance, 
       threshold_hard(distance,1000) AS ind_hard, 
       threshold_soft(distance,1000) AS ind_soft
 FROM od_closest 
WHERE dest_class = 'activity_centres';
'''.format(id = points_id)
           
 
supermarkets_1000 = '''
DROP TABLE IF EXISTS ind_supermarket1000;
CREATE TABLE ind_supermarket1000 AS
SELECT {id}, 
       array_agg(distance) AS distance_array,
       MIN(distance) AS distance,
       threshold_hard(MIN(distance),1000) AS ind_hard, 
       threshold_soft(MIN(distance),1000) AS ind_soft
FROM od_closest WHERE dest_class IN ('supermarket','supermarket_osm')
GROUP BY {id};
'''.format(id = points_id)
 
foodratio = '''
DROP TABLE IF EXISTS ind_foodratio;
CREATE TABLE ind_foodratio AS
SELECT p.{id}, 
       supermarkets.count AS supermarkets,
       fastfood.count AS fastfood,
       (CASE
        WHEN fastfood.count > 0 THEN
          (COALESCE(supermarkets.count,0))/(COALESCE(fastfood.count::double precision,0))
        WHEN fastfood.count IS NULL THEN
          (COALESCE(supermarkets.count,0)+1.0)/(COALESCE(fastfood.count::double precision,0)+1.0) 
        ELSE NULL END
        ) AS cond_food_ratio,            
       (COALESCE(supermarkets.count,0)+1.0)/(COALESCE(fastfood.count::double precision,0)+1.0) AS mod_food_ratio,
       log((COALESCE(supermarkets.count,0)+1.0)/(COALESCE(fastfood.count::double precision,0)+1.0)) AS log_mod_food_ratio,
       (CASE
        WHEN ((COALESCE(supermarkets.count,0))+(COALESCE(fastfood.count,0))) !=0 THEN  
          (COALESCE(supermarkets.count,0))/((COALESCE(supermarkets.count,0))+(COALESCE(fastfood.count,0)))::double precision
        ELSE NULL END) AS supermarket_proportion 
FROM parcel_dwellings p
LEFT JOIN (SELECT {id},COALESCE(count,0) AS count FROM od_counts WHERE dest_class = 'supermarket_osm') AS supermarkets 
  ON p.{id} = supermarkets.{id}
LEFT JOIN (SELECT {id},COALESCE(count,0) AS count FROM od_counts WHERE dest_class = 'fast food_osm') AS fastfood 
  ON p.{id} = fastfood.{id};
'''.format(id = points_id)  


     
create_neighbourhood_ind_list = {'01_daily_living': dl,
                                 '02_walkability': wa,
                                 '03_activity_centres': activity,
                                 '04_supermarkets_1000': supermarkets_1000,
                                 '05_foodratio': foodratio}
        


for ind in sorted(create_neighbourhood_ind_list.keys()):
  print("Creating {} indicator table... ".format(ind[3:len(ind)])),
  curs.execute(create_neighbourhood_ind_list[ind])
  conn.commit()
  print("Done.")

conn.close()

# output to completion log    
script_running_log(script, task, start, locale)
