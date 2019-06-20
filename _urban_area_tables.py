# Script:  _urban_area_tables.py
# Purpose: Urban subset area tables for map display purposes
# Author:  Carl Higgs
# Date:    20190620

import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import numpy
import time
import psycopg2 
from progressor import progressor
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *


# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create ABS and non-ABS linkage tables using 2016 data sourced from ABS'

# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()


## Note i still have doubts about person counts
for area in ['lga','ssc']:
    sql = '''
    DROP TABLE IF EXISTS study_region_urban_{area};
    CREATE TABLE study_region_urban_{area} AS
    SELECT {area}_name_2016,
           SUM(person) AS urban_person,
           SUM(dwelling) AS urban_dwelling,
           ST_Area(t.geom)/10000.0 AS urban_area_ha,
           t.geom
    FROM (SELECT b.{area}_name_2 AS {area}_name_2016,
               ST_Union(CASE
                  WHEN ST_CoveredBy(a.geom, b.geom)
                      THEN b.geom
                  ELSE
                      ST_CollectionExtract(ST_Multi(
                          ST_Intersection(a.geom, b.geom)
                          ),3) END) AS geom
           FROM study_region_urban a
     INNER JOIN main_{area}_2016_aust b
             ON (ST_Intersects(a.geom,b.geom))
        AND NOT ST_Touches(a.geom,b.geom)
       GROUP BY b.{area}_name_2) t
     LEFT JOIN abs_linkage c
             ON (ST_Intersects(t.geom,c.geom))
    GROUP BY {area}_name_2016,urban_area_ha, t.geom;
    CREATE INDEX IF NOT EXISTS study_region_urban_{area}_idx ON study_region_urban_{area} USING GIST (geom);
    SELECT SUM(urban_person),SUM(urban_dwelling),SUM(urban_area_ha) FROM study_region_urban_{area};
    '''.format(area = area)
    print(sql)

## The below needs to be heavily re-worked to ensure person and dwelling counts are accurate.

## May want to include sample point count and samples per Ha when this is working.

# create sa1 area linkage corresponding to later SA1 aggregate tables
print("  - SA1s")
create_area_sa1 = '''  
  DROP TABLE IF EXISTS area_sa1;
  CREATE TABLE area_sa1 AS
  SELECT DISTINCT ON (a.sa1_maincode) 
         a.sa1_maincode, 
         string_agg(distinct(b.ssc_name_2016),',') AS suburb, 
         string_agg(distinct(b.lga_name_2016), ', ') AS lga,
         COUNT(b.*) AS resid_parcels,
         SUM(a.dwelling) AS dwellings,
         SUM(a.person) AS resid_persons,
         ST_Union(
            CASE 
                WHEN ST_CoveredBy(c.geom, t.geom) 
                    THEN t.geom 
                ELSE 
                    ST_CollectionExtract(ST_Multi(
                        ST_Intersection(c.geom, t.geom)
                        ),3) END) AS geom
  FROM abs_linkage t 
  LEFT JOIN (SELECT sa1_maincode, 
                    ssc_name_2016, 
                    lga_name_2016,
             FROM parcel_dwellings 
             LEFT JOIN non_abs_linkage ON parcel_dwellings.gnaf_pid = non_abs_linkage.gnaf_pid
             LEFT JOIN abs_linkage ON parcel_dwellings.mb_code_20 = abs_linkage.mb_code_2016 
             ) b ON a.sa1_maincode = b.sa1_maincode
  LEFT JOIN (SELECT sa1_mainco, 
                    ST_Intersection(a.geom, b.geom) AS geom
             FROM main_sa1_2016_aust_full a, 
                  study_region_urban b) c ON a.sa1_maincode = c.sa1_mainco
  WHERE a.sa1_maincode IN (SELECT sa1_maincode FROM area_disadvantage)
  AND suburb IS NOT NULL 
  GROUP BY a.sa1_maincode
  ORDER BY a.sa1_maincode ASC;
  CREATE INDEX IF NOT EXISTS area_sa1_idx ON area_sa1 USING GIST (geom);
  '''.format(id = points_id)
curs.execute(create_area_sa1)
conn.commit()

# create Suburb area linkage (including geometry reflecting SA1 exclusions)
print("  - Suburbs")
create_area_ssc = '''  
  DROP TABLE IF EXISTS area_ssc;
  CREATE TABLE area_ssc AS
  SELECT DISTINCT ON (ssc_name_2016)
         ssc_name_2016 AS suburb, 
         --string_agg(distinct(lga_name_2016), ', ') AS lga,
         --sum(resid_parcels) AS resid_parcels,
         --sum(dwelling) AS dwellings,
         --sum(person) AS resid_persons,
         ST_Union(
            CASE 
                WHEN ST_CoveredBy(c.geom, t.geom) 
                    THEN t.geom 
                ELSE 
                    ST_CollectionExtract(ST_Multi(
                        ST_Intersection(c.geom, t.geom)
                        ),3) END) AS geom
  FROM  (SELECT DISTINCT ON (mb_code_2016)
                mb_code_2016,
                ssc_name_2016,
                lga_name_2016,
                COUNT(*) AS resid_parcels,
                dwelling,
                person,
                a.geom AS geom
         FROM abs_linkage a
         LEFT JOIN parcel_dwellings p ON a.mb_code_2016 = p.mb_code_20
         LEFT JOIN non_abs_linkage b on p.gnaf_pid = b.gnaf_pid
         WHERE a.sa1_maincode IN (SELECT sa1_maincode FROM area_disadvantage)
         AND ssc_name_2016 IS NOT NULL
         GROUP BY mb_code_2016,ssc_name_2016,lga_name_2016,dwelling,person,a.geom
         ) t
  LEFT JOIN (SELECT ssc_name_2, 
                    CASE 
                       WHEN ST_CoveredBy(a.geom, b.geom) 
                           THEN b.geom 
                       ELSE 
                           ST_CollectionExtract(ST_Multi(
                               ST_Intersection(a.geom, b.geom)
                               ),3) END AS geom
             FROM study_region_urban a
       INNER JOIN main_ssc_2016_aust b
               ON (ST_Intersects(a.geom,b.geom))) c ON t.ssc_name_2016 = c.ssc_name_2
  GROUP BY suburb
  ORDER BY suburb ASC;
  CREATE INDEX IF NOT EXISTS area_ssc_idx ON area_ssc USING GIST (geom);
  '''.format(points_id)  
curs.execute(create_area_ssc)
conn.commit()
  
# create LGA table corresponding to later SA1 aggregate tables
print("  - LGAs")
create_area_lga = '''  
  DROP TABLE IF EXISTS area_lga;
  SELECT DISTINCT ON (lga_name_2016)
         lga_name_2016 AS lga,
         sum(resid_parcels) AS resid_parcels,
         sum(dwelling) AS dwellings,
         sum(person) AS resid_persons, 
         ST_Union(
            CASE 
                WHEN ST_CoveredBy(c.geom, t.geom) 
                    THEN t.geom 
                ELSE 
                    ST_CollectionExtract(ST_Multi(
                        ST_Intersection(c.geom, t.geom)
                        ),3) END) AS geom
  FROM  (SELECT DISTINCT ON (mb_code_2016)
                mb_code_2016,
                lga_name_2016,
                COUNT(*) AS resid_parcels,
                dwelling,
                person,
                a.geom AS geom
         FROM abs_linkage a
         LEFT JOIN parcel_dwellings p ON a.mb_code_2016 = p.mb_code_20
         LEFT JOIN non_abs_linkage b on p.gnaf_pid = b.gnaf_pid
         WHERE a.sa1_maincode IN (SELECT sa1_maincode FROM area_disadvantage)
         AND lga_name_2016 IS NOT NULL
         GROUP BY mb_code_2016,lga_name_2016,dwelling,person,a.geom
         ) t
  LEFT JOIN (SELECT lga_name_2,  
                    CASE 
                       WHEN ST_CoveredBy(a.geom, b.geom) 
                           THEN b.geom 
                       ELSE 
                           ST_CollectionExtract(ST_Multi(
                               ST_Intersection(a.geom, b.geom)
                               ),3) END AS geom
             FROM study_region_urban a
             INNER JOIN main_lga_2016_aust b
             ON (ST_Intersects(a.geom,b.geom))) c ON t.lga_name_2016 = c.lga_name_2
  GROUP BY lga
  ORDER BY lga ASC;
  CREATE INDEX IF NOT EXISTS area_lga_idx ON area_lga USING GIST (geom);
  '''.format(points_id)
curs.execute(create_area_lga)
conn.commit()