# Script:  17_aedc_measures.py
# Purpose: Create parcel indicators for national liveability project
# Author:  Carl Higgs 
# Date:    20180717

import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

# Get a list of destinations processed within this region for distance to closest
sql = '''SELECT DISTINCT(dest_name) dest_name FROM od_closest ORDER BY dest_name;'''
curs.execute(sql)
categories = [x[0] for x in curs.fetchall()]
category_list = ','.join(categories)
category_types = '"{}" int'.format('" int, "'.join(categories))
sql = '''SELECT DISTINCT(dest_class) FROM od_distances_3200m ORDER BY dest_class;'''
curs.execute(sql)
array_categories = [x[0] for x in curs.fetchall()]
array_category_list = ','.join(array_categories)
array_category_types = '"{}" int[]'.format('" int[], "'.join(array_categories))

print("Create summary table of destination distances... "),
crosstab = '''
DROP TABLE IF EXISTS dest_distance_m;
CREATE TABLE IF NOT EXISTS dest_distance_m AS
SELECT *
  FROM   crosstab(
   'SELECT gnaf_pid, lower(dest_name), distance
    FROM   od_closest
    ORDER  BY 1,2'  -- could also just be "ORDER BY 1" here
  ,$$SELECT unnest('{curly_o}{category_list}{curly_c}'::text[])$$
   ) AS distance ("gnaf_pid" text, {category_types});
'''.format(id = points_id.lower(),
           curly_o = "{",
           curly_c = "}",
           category_list = category_list.lower(),
           category_types = category_types.lower())
curs.execute(crosstab)
conn.commit()
print("Done.")

print("Create summary table of destination distance arrays... "),
crosstab = '''
DROP TABLE IF EXISTS dest_distances_3200m;
CREATE TABLE IF NOT EXISTS dest_distances_3200m AS
SELECT *
  FROM   crosstab(
   'SELECT gnaf_pid, lower(dest_class), distances
    FROM   od_distances_3200m
    ORDER  BY 1,2'  -- could also just be "ORDER BY 1" here
  ,$$SELECT unnest('{curly_o}{category_list}{curly_c}'::text[])$$
   ) AS distances ("gnaf_pid" text, {category_types});
'''.format(id = points_id.lower(),
           curly_o = "{",
           curly_c = "}",
           category_list = array_category_list.lower(),
           category_types = array_category_types.lower())
curs.execute(crosstab)
conn.commit()
print("Done.")

print("Ensure dest distance table has all required fields (you may have to edit the script on from line 57!)... "),
dest_distance_m = []
for dest in df_destinations.destination.tolist():
  sql = '''ALTER TABLE dest_distance_m ADD COLUMN IF NOT EXISTS {} int;'''.format(dest)
  curs.execute(sql)
  conn.commit()
  dest_distance_m.append(''' dest_distance_m.{dest} as dist_cl_{dest}, '''.format(dest = dest))
print("Done.")

print("Ensure dest distances 3200 table has all required fields (you may have to edit the script on from line 182!)... "),
dest_distances_3200m = []
for dest_class in df_destinations.destination_class.tolist():
  sql = '''ALTER TABLE dest_distances_3200m ADD COLUMN IF NOT EXISTS {} int[] ;'''.format(dest_class)
  curs.execute(sql)
  conn.commit()
  dest_distances_3200m.append(' dest_distances_3200m.{dest_class} as dist_3200m_{dest_class}, '.format(dest_class))
print("Done.")

aedc_measures = '''
DROP TABLE IF EXISTS aedc_measures;
CREATE TABLE aedc_measures AS
SELECT
p.gnaf_pid ,
'{locale}' as locale,
p.count_objectid ,
p.point_x ,
p.point_y ,
p.hex_id ,
abs.mb_code_2016 ,
abs.mb_category_name_2016,
abs.dwelling ,
abs.person ,
abs.sa1_maincode ,
abs.sa2_name_2016 ,
abs.sa3_name_2016 ,
abs.sa4_name_2016 ,
abs.gccsa_name ,
abs.state_name ,
non_abs.ssc_code_2016 ,
non_abs.ssc_name_2016 ,
non_abs.lga_code_2016 ,
non_abs.lga_name_2016 ,
sos.sos_name_2016 ,
ind_activity.distance/1000.0 as walk_12,
sc_nh1600m.sc_nh1600m as walk_15,
dd_nh1600m.dd_nh1600m as walk_16,
ind_walkability.wa_soft as walk_17_soft,
ind_walkability.wa_hard as walk_17_hard,
{dest_distance_m}
{dest_distnces_3200m}
od_aos_jsonb.attributes as aos_distances,
p.geom
FROM
parcel_dwellings p
LEFT JOIN abs_linkage abs ON p.mb_code_20 = abs.mb_code_2016
LEFT JOIN non_abs_linkage non_abs ON p.gnaf_pid = non_abs.gnaf_pid
LEFT JOIN parcel_sos sos ON p.gnaf_pid = sos.gnaf_pid
LEFT JOIN ind_activity ON p.gnaf_pid = ind_activity.gnaf_pid
LEFT JOIN sc_nh1600m ON p.gnaf_pid = sc_nh1600m.gnaf_pid
LEFT JOIN dd_nh1600m ON p.gnaf_pid = dd_nh1600m.gnaf_pid
LEFT JOIN ind_walkability ON p.gnaf_pid = ind_walkability.gnaf_pid
LEFT JOIN ind_transport ON p.gnaf_pid = ind_transport.gnaf_pid
LEFT JOIN ind_pos_closest ON p.gnaf_pid = ind_pos_closest.gnaf_pid
LEFT JOIN od_aos_jsonb ON p.gnaf_pid = od_aos_jsonb.gnaf_pid
LEFT JOIN dest_distance_m ON p.gnaf_pid = dest_distance_m.gnaf_pid
LEFT JOIN dest_distances_3200m ON p.gnaf_pid = dest_distances_3200m.gnaf_pid;
CREATE UNIQUE INDEX aedc__measures_idx ON aedc_measures (gnaf_pid);  
'''.format(locale=locale)
curs.execute(aedc_measures)
conn.commit()

print('''Analyse the AEDC measures table... '''),
curs.execute('''ANALYZE aedc_measures;''')
conn.commit()
print("Done.")

print('''
Prepare report table (aedc_null_fraction) on proportion of rows that are null.  That is,
  - if null_fract is 1 for a variable, then 100% are null.  Please check:
      - perhaps no destinations of this type in your region?
      - or some processing stage has been missed?
  - if null_fract is .01 for a variable, then 1% are null  (which is still quite large and worth investigating)
  - if null_fract is .0001 for a variable, then 1 in 10000 are null which may be realistic
''')
null_check = '''
DROP TABLE IF EXISTS aedc_null_fraction;
CREATE TABLE aedc_null_fraction AS
SELECT locale.locale, 
       attname,
       null_frac 
FROM pg_stats,
     (SELECT locale::text FROM aedc_measures LIMIT 1) locale 
WHERE pg_stats."tablename" = 'aedc_measures';
'''
curs.execute(null_check)
conn.commit()
print("Done.")

print('''Add locale column to open_space_areas in preparation for merge with other data... '''),
aos_locale = '''
ALTER TABLE open_space_areas ADD COLUMN IF NOT EXISTS locale text;
UPDATE open_space_areas SET locale = '{}' ;
'''.format(locale.lower())
curs.execute(aos_locale)
conn.commit()
print("Done.")

print("Exporting aedc measures, null fraction check, and open space areas to study region's data directory..."),
# command = '''
# pg_dump -U postgres -h localhost -W  -t "aedc_measures" -t "aedc_null_fraction" -t "open_space_areas" {db} > aedc_{db}.sql
# '''.format(locale = locale.lower(), year = year,db = db)
# sp.call(command, shell=True, cwd=folderPath)                           
for table in ['aedc_measures','aedc_null_fraction','open_space_areas']:
  file = os.path.join(locale_dir,'{db}_{table}.csv'.format(db = db,table = table))
  with open(file, 'w') as f:
    sql = '''COPY {table} TO STDOUT WITH DELIMITER ';' CSV HEADER;'''.format(table = table)
    curs.copy_expert(sql,f)
  
print("Done.")

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
