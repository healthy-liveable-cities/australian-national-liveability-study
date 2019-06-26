import os
import sys
import time
import psycopg2 

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

date = time.strftime("%Y%m%d")
table = 'kai_highlife_{}_{}'.format(locale,date)
print("Creating custom table for {}".format(locale))
kai_query = '''
DROP TABLE IF EXISTS {table};
CREATE TABLE {table} AS
SELECT DISTINCT ON (sa1_maincode)
       a.sa1_maincode,
       -- note that SA1 statistics dwelling and person (constructed from MB data) are common
       -- across all samples within each distinct SA1, so the average is equal to the constant common value
       a.dwelling                         AS dwelling                    ,
       a.person                           AS person                      ,
       p.sample_points                                                   ,
       p.dwelling_density                                                ,
       p.street_connectivity                                             ,
       p.daily_living_1600                                               ,
       p.local_living_1600                                               ,
       p.walkability                                                     ,
       p.pos_any_distance_m                                              ,
       p.pos_5k_sqm_distance_m                                           ,
       p.pos_15k_sqm_distance_m                                          ,
       p.pos_20k_sqm_distance_m                                          ,
       p.pos_4k_10k_sqm_distance_m                                       ,
       p.pos_10k_50k_sqm_distance_m                                      ,
       p.pos_50k_200k_sqm_distance_m                                     ,
       p.pos_50k_sqm_distance_m                                          ,
       p.sport_distance_m                                                ,
       p.pos_toilet_distance_m                                           ,
       playgrounds.count                  AS playgrounds                 ,
       abs.geom
FROM (SELECT sa1_maincode, 
                  sum(dwelling) AS dwelling, 
                  sum(person)   AS person 
            FROM abs_linkage 
           GROUP BY sa1_maincode) a
LEFT JOIN (SELECT
       sa1_maincode, 
       COUNT(*)                           AS sample_points               ,
       AVG(walk_18)                       AS dwelling_density            ,
       AVG(walk_19)                       AS street_connectivity         ,
       AVG(walk_20_soft)                  AS daily_living_1600           ,
       AVG(walk_21_soft)                  AS local_living_1600           ,
       AVG(walk_22_soft)                  AS walkability                 ,
       AVG(o.pos_any_distance_m         ) AS pos_any_distance_m          ,
       AVG(o.pos_5k_sqm_distance_m      ) AS pos_5k_sqm_distance_m       ,
       AVG(o.pos_15k_sqm_distance_m     ) AS pos_15k_sqm_distance_m      ,
       AVG(o.pos_20k_sqm_distance_m     ) AS pos_20k_sqm_distance_m      ,
       AVG(o.pos_4k_10k_sqm_distance_m  ) AS pos_4k_10k_sqm_distance_m   ,
       AVG(o.pos_10k_50k_sqm_distance_m ) AS pos_10k_50k_sqm_distance_m  ,
       AVG(o.pos_50k_200k_sqm_distance_m) AS pos_50k_200k_sqm_distance_m ,
       AVG(o.pos_50k_sqm_distance_m     ) AS pos_50k_sqm_distance_m      ,
       AVG(o.sport_distance_m           ) AS sport_distance_m            ,
       AVG(o.pos_toilet_distance_m      ) AS pos_toilet_distance_m
       FROM parcel_indicators 
       LEFT JOIN ind_os_distance o on parcel_indicators.gnaf_pid = o.gnaf_pid
       WHERE exclude IS NULL
       GROUP BY sa1_maincode
       ) p USING (sa1_maincode)
LEFT JOIN (SELECT * 
             FROM sa1_dest_counts 
            WHERE dest_class = 'playgrounds'
            ) playgrounds on p.sa1_maincode = playgrounds.sa1_mainco
LEFT JOIN main_sa1_2016_aust_full abs ON a.sa1_maincode = abs.sa1_mainco
WHERE p.sample_points > 0
ORDER BY a.sa1_maincode;                                 
'''.format(table = table, id = points_id.lower())
curs.execute(kai_query)
conn.commit()
    
# print('''
# Please enter the following in psql in order to output the table to csv:
# COPY {table} TO 'D:/ntnl_li_2018_template/data/{table}.csv' WITH DELIMITER ',' CSV HEADER;                                         
# '''.format(table = table))
    
print("Copying table to geopackage..."),
command = (
            'ogr2ogr -overwrite -f GPKG {path}/{output_name}.gpkg '
            'PG:"host={host} user={user} dbname={db} password={pwd}" '
            '  {tables}'
            ).format(output_name = table,
                     path =  'D:/ntnl_li_2018_template/data/',
                     host = db_host,
                     user = db_user,
                     pwd = db_pwd,
                     db = db,
                     tables = table) 
print(" Done.")
sp.call(command, shell=True) 


