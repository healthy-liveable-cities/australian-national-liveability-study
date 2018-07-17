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


dl = '''
DROP TABLE IF EXISTS ind_daily_living;
CREATE TABLE IF NOT EXISTS ind_daily_living AS
SELECT p.{id}, 
       convenience.ind_hard + supermarkets.ind_hard + any_pt.ind_hard AS dl_hard,
       convenience.ind_soft + supermarkets.ind_soft + any_pt.ind_soft AS dl_soft                
FROM parcel_dwellings p  
LEFT JOIN (SELECT {id}, 
                  (CASE WHEN SUM(COALESCE(ind_hard,0)) > 0 THEN 1 ELSE 0 END) AS ind_hard,
                  MAX(COALESCE(ind_soft,0)) AS ind_soft
          FROM od_closest WHERE dest IN (2,3,4) GROUP BY {id}) convenience  
  ON p.{id} = convenience.{id}
LEFT JOIN (SELECT {id}, ind_hard, ind_soft
           FROM od_closest WHERE dest = 6) supermarkets on p.{id} = supermarkets.{id}
LEFT JOIN (SELECT {id}, ind_hard, ind_soft 
           FROM od_closest WHERE dest = 8) any_pt 
  ON p.{id} = any_pt.{id};
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
             (dl_hard - AVG(dl_hard) OVER())/stddev_pop(dl_hard) OVER() as z_dl_hard,
             (dl_soft - AVG(dl_soft) OVER())/stddev_pop(dl_soft) OVER() as z_dl_soft FROM ind_daily_living) dl
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
SELECT {id},distance FROM od_closest WHERE dest = 100;
'''.format(id = points_id)
           
 
 
supermarkets_1000 = '''
DROP TABLE IF EXISTS ind_supermarket1000;
CREATE TABLE ind_supermarket1000 AS
SELECT {id}, 
       distance,
       (CASE 
        WHEN distance <  1000  THEN 1
        WHEN distance >= 1000 THEN 0
        ELSE NULL END) as ind_sm1000_hard,
        1-1/(1+exp(-5*(distance-1000)/1000::double precision)) AS ind_sm1000_soft
FROM od_closest WHERE dest = 6;
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
LEFT JOIN (SELECT {id},count FROM od_counts WHERE dest = 6) AS supermarkets 
  ON p.{id} = supermarkets.{id}
LEFT JOIN (SELECT {id},count FROM od_counts WHERE dest = 5) AS fastfood 
  ON p.{id} = fastfood.{id};
'''.format(id = points_id)  


     
create_neighbourhood_ind_list = {'daily_living': dl,
                                 'walkability': wa,
                                 'activity_centres': activity,
                                 'supermarkets_1000': supermarkets_1000,
                                 'foodratio': foodratio}
        
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

for ind in create_neighbourhood_ind_list.keys():
  print("Creating {} indicator table... ".format(ind)),
  curs.execute(create_neighbourhood_ind_list[ind])
  conn.commit()
  print("Done.")

conn.close()

# output to completion log    
script_running_log(script, task, start, locale)
