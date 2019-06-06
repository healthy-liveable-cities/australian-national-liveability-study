# Script:  44_create_older_person_Services_index.py
# Purpose: calculate older person services index using config file for id variable
# Author:  Carl Higgs 
# Date:    20170418

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
task = 'calculate access to services for older persons index using config file for id variable'

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

area = "sa1_maincode"

create_ind_older_services = '''
DROP TABLE IF EXISTS ind_older_services;
CREATE TABLE ind_older_services AS
SELECT {id}, 
       z_ac, 
       z_acr, 
       z_cc_lib, 
       z_gp, 
       z_ho, 
       z_sup, 
       z_u3a, 
       z_pt, 
       z_pow, 
       (z_ac + z_acr + z_cc_lib + z_gp + z_ho + z_sup + z_u3a + z_pt + z_pow) AS older_services
FROM (SELECT {id},
            ("nhsd_2017_aged_care"             - AVG("nhsd_2017_aged_care"            ) OVER())/stddev_pop("nhsd_2017_aged_care"            ) OVER() as z_ac,
            ("nhsd_2017_aged_care_residential" - AVG("nhsd_2017_aged_care_residential") OVER())/stddev_pop("nhsd_2017_aged_care_residential") OVER() as z_acr,
            ("community_centre_library"        - AVG("community_centre_library"       ) OVER())/stddev_pop("community_centre_library"       ) OVER() as z_cc_lib,
            ("nhsd_2017_gp"                    - AVG("nhsd_2017_gp"                   ) OVER())/stddev_pop("nhsd_2017_gp"                   ) OVER() as z_gp, 
            ("nhsd_2017_hospital"              - AVG("nhsd_2017_hospital"             ) OVER())/stddev_pop("nhsd_2017_hospital"             ) OVER() as z_ho,
            ("supermarket_osm"                 - AVG("supermarket_osm"                ) OVER())/stddev_pop("supermarket_osm"                ) OVER() as z_sup, 
            ("u3a"                             - AVG("u3a"                            ) OVER())/stddev_pop("u3a"                            ) OVER() as z_u3a, 
            (gtfs_2018_stops                   - AVG(gtfs_2018_stops                  ) OVER())/stddev_pop(gtfs_2018_stops                  ) OVER() as z_pt,
            (place_of_worship_osm              - AVG(place_of_worship_osm             ) OVER())/stddev_pop(place_of_worship_osm             ) OVER() as z_pow
      FROM (SELECT {id}                                                                                 ,
                   threshold_soft("nhsd_2017_aged_care"                ,1600)  AS "nhsd_2017_aged_care"               ,
                   threshold_soft("nhsd_2017_aged_care_residential"    ,1600)  AS "nhsd_2017_aged_care_residential"   ,
                   threshold_soft(
                            LEAST("community_centre_mildura_2019","libraries_2018")
                                                                       ,1600)  AS "community_centre_library"          ,
                   threshold_soft("nhsd_2017_gp"                       ,1600)  AS "nhsd_2017_gp"                      ,
                   threshold_soft("nhsd_2017_hospital"                 ,1600)  AS "nhsd_2017_hospital"                ,
                   threshold_soft("supermarket_osm"                    ,1600)  AS "supermarket_osm"                   ,
                   threshold_soft("u3a_mildura_2019"                   ,1600)  AS "u3a"                               ,
                   threshold_soft("gtfs_2018_stops"                    ,1600)  AS "gtfs_2018_stops"                   ,
                   threshold_soft("place_of_worship_osm"               ,1600)  AS "place_of_worship_osm" 
              FROM dest_distance_m
              WHERE NOT EXISTS 
                     -- Apply exclusions as determined in scripting process based on SOS, SEIFA IRSD and key indicators
                    (SELECT 1 
                    FROM excluded_parcels e
                   WHERE dest_distance_m.{id} = e.{id})
                AND EXISTS 
                    -- Only include those parcels in the included parcels table 'study_parcels'
                    (SELECT 1 
                    FROM study_parcels s
                   WHERE dest_distance_m.{id} = s.{id})
              )
                t) as t_outer;

DROP TABLE IF EXISTS ind_older_services_sa1;
CREATE TABLE ind_older_services_sa1 AS
SELECT t.{area}  ,
       sample_point_count,
       t.z_ac ,
       t.z_acr,
       t.z_cc_lib ,
       t.z_gp ,
       t.z_ho ,
       t.z_sup,
       t.z_u3a,
       t.z_pt ,    
       t.z_pow,        
       t.older_services,
       ntile(10) OVER(ORDER BY z_ac  DESC) as ac_decile,
       ntile(10) OVER(ORDER BY z_acr  DESC) as acr_decile,
       ntile(10) OVER(ORDER BY z_cc_lib  DESC) as cc_decile,
       ntile(10) OVER(ORDER BY z_gp  DESC) as gp_decile,
       ntile(10) OVER(ORDER BY z_ho  DESC) as ho_decile,
       ntile(10) OVER(ORDER BY z_sup DESC) as sup_decile,
       ntile(10) OVER(ORDER BY z_u3a DESC) as u3a_decile,
       ntile(10) OVER(ORDER BY z_pt  DESC) as pt_decile,
       ntile(10) OVER(ORDER BY z_pow  DESC) as pow_decile,
       ntile(10) OVER(ORDER BY older_services  DESC) as older_services_decile 
FROM (SELECT abs_linkage.{area},
             COUNT(*) AS sample_point_count,
             AVG(z_ac ) AS z_ac ,
             AVG(z_acr ) AS z_acr ,
             AVG(z_cc_lib ) AS z_cc_lib ,
             AVG(z_gp ) AS z_gp ,
             AVG(z_ho ) AS z_ho ,
             AVG(z_sup) AS z_sup,
             AVG(z_u3a) AS z_u3a,
             AVG(z_pt)  AS z_pt,
             AVG(z_pow)  AS z_pow,
             AVG(older_services) AS older_services
      FROM parcel_dwellings p
      LEFT JOIN abs_linkage ON p.mb_code_20 = abs_linkage.mb_code_2016
      LEFT JOIN ind_older_services ON p.{id} = ind_older_services.{id}
      WHERE ind_older_services.{id} IS NOT NULL
      GROUP BY abs_linkage.{area}
      ) as t;
  '''.format(id = points_id.lower(),area = area)        

 
curs.execute(create_ind_older_services)
conn.commit()

# output to completion log    
script_running_log(script, task, start)
conn.close()