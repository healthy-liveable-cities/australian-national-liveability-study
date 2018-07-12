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

create_dl = '''
DROP TABLE IF EXISTS ind_daily_living;
CREATE TABLE IF NOT EXISTS ind_daily_living AS
SELECT p.{id}, 
       convenience.ind_hard + supermarkets.ind_hard + any_pt.ind_hard AS dl_hard,
       convenience.ind_soft + supermarkets.ind_soft + any_pt.ind_soft AS dl_soft                
FROM parcel_dwellings p  
LEFT JOIN (SELECT {id}, 
                  (CASE WHEN SUM(COALESCE(ind_hard,0)) > 0 THEN 1 ELSE 0 END) AS ind_hard,
                  AVG(COALESCE(ind_soft,0)) AS ind_soft
FROM od_closest WHERE dest IN (2,3,4) GROUP BY {id}) convenience  ON p.{id} = convenience.{id}
LEFT JOIN (SELECT {id}, 
                  (CASE 
                   WHEN distance <  1600  THEN 1
                   WHEN distance >= 1600 THEN 0
                   ELSE NULL END) as ind_hard,
                   1-1/(1+exp(-5*(distance-1600)/1600::double precision)) AS ind_soft
FROM od_closest WHERE dest = 6) supermarkets on p.{id} = supermarkets.{id}
LEFT JOIN (SELECT {id}, ind_hard, ind_soft FROM od_closest WHERE dest = 8) any_pt 
      ON p.{id} = any_pt.{id};
'''.format(id = points_id)

create_ind_walkability = '''
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
        ON dd.{id} = dl.{id};'''.format(id = points_id)
        
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()
curs.execute(create_dl)
conn.commit()
curs.execute(create_ind_walkability)
conn.commit()
conn.close()

# output to completion log    
script_running_log(script, task, start, locale)
