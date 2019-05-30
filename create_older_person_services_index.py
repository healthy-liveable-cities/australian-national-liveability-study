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
       z_cc, 
       z_gp, 
       z_ho, 
       z_lib, 
       z_sup, 
       z_u3a, 
       z_pt, 
       z_pow, 
       (z_ac + z_acf + z_cc + z_gp + z_ho + z_lib + z_sup + z_u3a + z_pt + z_pow) AS older_services
FROM (SELECT {id},
            ("AgedCare" - AVG("AgedCare") OVER())/stddev_pop("AgedCare") OVER() as z_ac,
            ("AgedCareResidentialServices" - AVG("AgedCareResidentialServices") OVER())/stddev_pop("AgedCareResidentialServices") OVER() as z_acr,
            ("community_centre_osm" - AVG("community_centre_osm") OVER())/stddev_pop("community_centre_osm") OVER() as z_cc,
            ("GeneralPracticeGP_GP" - AVG("GeneralPracticeGP_GP") OVER())/stddev_pop("GeneralPracticeGP_GP") OVER() as z_gp, 
            ("Hospital" - AVG("Hospital") OVER())/stddev_pop("Hospital") OVER() as z_ho,
            ("libraries_2018" - AVG("libraries_2018") OVER())/stddev_pop("libraries_2018") OVER() as z_lib, 
            ("supermarket_osm" - AVG("supermarket_osm") OVER())/stddev_pop("supermarket_osm") OVER() as z_sup, 
            ("U3A" - AVG("U3A") OVER())/stddev_pop("U3A") OVER() as z_u3a, 
            (gtfs_2018_stops - AVG(gtfs_2018_stops) OVER())/stddev_pop(gtfs_2018_stops) OVER() as z_pt,
            (place_of_worship_osm - AVG(place_of_worship_osm) OVER())/stddev_pop(place_of_worship_osm) OVER() as z_pow
      FROM (SELECT {id}                                                                                 ,
                   threshold_soft("AgedCare"                    ,1600)  AS "AgedCare"                      ,
                   threshold_soft("AgedCareResidentialServices" ,1600)  AS "AgedCareResidentialServices"   ,
                   threshold_soft("community_centre_osm"        ,1600)  AS "community_centre_osm"          ,
                   threshold_soft("GeneralPracticeGP_GP"        ,1600)  AS "GeneralPracticeGP_GP"          ,
                   threshold_soft("Hospital"                    ,1600)  AS "Hospital"                      ,
                   threshold_soft("libraries_2018"              ,1600)  AS "libraries_2018"                ,
                   threshold_soft("supermarket_osm"             ,1600)  AS "supermarket_osm"               ,
                   threshold_soft("U3A"                         ,1600)  AS "U3A"                           ,
                   threshold_soft("gtfs_2018_stops"             ,1600)  AS "gtfs_2018_stops"               ,
                   threshold_soft("place_of_worship_osm"        ,1600)  AS "place_of_worship_osm" 
              FROM dest_distance_m)
                t) as t_outer;

DROP TABLE IF EXISTS ind_older_services_sa1;
CREATE TABLE ind_older_services_sa1 AS
SELECT t.{area}  ,
       t.z_ac ,
       t.z_acr,
       t.z_cc ,
       t.z_gp ,
       t.z_ho ,
       t.z_lib,
       t.z_sup,
       t.z_u3a,
       t.z_pt ,    
       t.z_pow,        
       t.older_services,
       ntile(10) OVER(ORDER BY z_ac  DESC) as ac_decile,
       ntile(10) OVER(ORDER BY z_acr  DESC) as acr_decile,
       ntile(10) OVER(ORDER BY z_cc  DESC) as cc_decile,
       ntile(10) OVER(ORDER BY z_gp  DESC) as gp_decile,
       ntile(10) OVER(ORDER BY z_ho  DESC) as ho_decile,
       ntile(10) OVER(ORDER BY z_lib DESC) as lib_decile,
       ntile(10) OVER(ORDER BY z_sup DESC) as sup_decile,
       ntile(10) OVER(ORDER BY z_u3a DESC) as u3a_decile,
       ntile(10) OVER(ORDER BY z_pt  DESC) as pt_decile,
       ntile(10) OVER(ORDER BY z_pow  DESC) as pow_decile,
       ntile(10) OVER(ORDER BY older_services  DESC) as older_services_decile 
FROM (SELECT abs_linkage.{area},
                    AVG(z_ac ) AS z_ac ,
                    AVG(z_ac ) AS z_acr ,
                    AVG(z_cc ) AS z_cc ,
                    AVG(z_gp ) AS z_gp ,
                    AVG(z_ho ) AS z_ho ,
                    AVG(z_lib) AS z_lib,
                    AVG(z_sup) AS z_sup,
                    AVG(z_u3a) AS z_u3a,
                    AVG(z_pt)  AS z_pt,
                    AVG(z_pow)  AS z_pow,
                    AVG(older_services) AS older_services
                    FROM parcel_dwellings p
                    LEFT JOIN abs_linkage ON p.mb_code_20 = abs_linkage.mb_code_2016
                    LEFT JOIN ind_older_services ON p.{id} = ind_older_services.{id}
                    GROUP BY abs_linkage.{area}) as t;
  '''.format(id = points_id.lower(),area = area)        

 
curs.execute(create_ind_older_services)
conn.commit()

# output to completion log    
script_running_log(script, task, start)
conn.close()