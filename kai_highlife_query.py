import os
import sys
import time
import psycopg2 

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

date = time.strftime("%Y%m%d-%H%M")
table = 'kai_highlife_{}'.format(date)
print("Creating custom table 
kai_query = '''
DROP TABLE IF EXISTS {table} IF EXISTS;
CREATE TABLE {table} AS
SELECT p.sa1_maincode                                                    ,
       SUM(p.dwelling)                    AS dwelling                    ,
       SUM(p.person)                      AS person                      ,
       AVG(p.walk_18)                     AS dwelling_density            ,
       AVG(p.walk_19)                     AS street_connectivity         ,
       AVG(p.walk_20_soft)                AS daily_living_1600           ,
       AVG(p.walk_21_soft)                AS local_living_1600           ,
       AVG(p.walk_22_soft)                AS walkability                 ,
       SUM(playgrounds.count)             AS playgrounds                 ,
       AVG(o.pos_any_distance_m         ) AS pos_any_distance_m          ,
       AVG(o.pos_5k_sqm_distance_m      ) AS pos_5k_sqm_distance_m       ,
       AVG(o.pos_15k_sqm_distance_m     ) AS pos_15k_sqm_distance_m      ,
       AVG(o.pos_20k_sqm_distance_m     ) AS pos_20k_sqm_distance_m      ,
       AVG(o.pos_4k_10k_sqm_distance_m  ) AS pos_4k_10k_sqm_distance_m   ,
       AVG(o.pos_10k_50k_sqm_distance_m ) AS pos_10k_50k_sqm_distance_m  ,
       AVG(o.pos_50k_200k_sqm_distance_m) AS pos_50k_200k_sqm_distance_m ,
       AVG(o.pos_50k_sqm_distance_m     ) AS pos_50k_sqm_distance_m      ,
       AVG(o.sport_distance_m           ) AS sport_distance_m            ,
       AVG(o.pos_toilet_distance_m      ) AS pos_toilet_distance_m       ,
       abs.geom
FROM parcel_indicators p
LEFT JOIN ind_os_distance o on p.gnaf_pid = o.gnaf_pid
LEFT JOIN sa1_dest_counts playgrounds on p.sa1_maincode = playgrounds.sa1_mainco
LEFT JOIN main_sa1_2016_aust_full abs ON p.sa1_maincode = abs.sa1_mainco
WHERE playgrounds.dest_class = 'playgrounds'
GROUP BY p.sa1_maincode,abs.geom
ORDER BY p.sa1_maincode;
COPY {table} TO 'D:/ntnl_li_2018_template/data/{table}.csv' WITH DELIMITER ',' CSV HEADER;                                         
'''.format(table = table)
curs.execute(kai_query)
conn.commit()
    
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


