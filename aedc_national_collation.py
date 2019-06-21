# Script:  aedc_national_collation.py
# Purpose: Collate study region csv output for aedc measures
# Author:  Carl Higgs 
# Date:    20180717
# Note:    Assumes the li_australia_2018 database has been created.  
import time
import psycopg2 
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

# Connect to postgresql database     
db = 'li_australia_2018'
aedc_dir = os.path.join(folderPath,'study_region','aedc')
print("This script assumes the database {db} has been created!".format(db = db))
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

curs.execute('''SELECT 1 WHERE to_regclass('public.aedc_indicators_aifs') IS NOT NULL;''')
res = curs.fetchone()
if res:
    print("AEDC AIFS table already exists.")
    sql = '''SELECT DISTINCT(locale) FROM aedc_indicators_aifs ORDER BY locale;'''
    curs.execute(sql)
    processed_locales = [x[0] for x in curs.fetchall()]
        
else:
    print("Create empty AEDC AIFS tables... ")
    command = 'psql li_australia_2018 < aedc_aifs_schema.sql'
    sp.call(command, shell=True,cwd=aedc_dir)   
    processed_locales = []
    print("Done.")

print("Drop table indices, if existing... "),
sql = '''
DROP INDEX IF EXISTS aedc_indicators_aifs_idx;
DROP INDEX IF EXISTS aedc_indicators_aifs_gix;
DROP INDEX IF EXISTS aos_acara_naplan_idx    ;
DROP INDEX IF EXISTS aos_idx                 ;
DROP INDEX IF EXISTS idx_aos_jsb             ;
'''.format(id = points_id.lower())
curs.execute(sql)
conn.commit()
print("Done.")

print("Looping over study regions to check if required output exists in their folder; if it does, its imported...")
for locale in study_regions:
  sql = 'aedc_aifs_li_{}_2018_Fc.sql'.format(locale)
  if locale in processed_locales:
    print(" - {} (previously processed)".format(locale))
  elif os.path.isfile(os.path.join(aedc_dir,sql)):
    print(" - {}".format(locale)),
    command = 'pg_restore -a -Fc -d li_australia_2018 < {}'.format(sql)
    sp.call(command, shell=True,cwd=aedc_dir)   
    sql = '''SELECT (SELECT COUNT(*) FROM aedc_indicators_aifs WHERE locale = 'albury_wodonga') AS sample_points,(SELECT count(*) from aedc_indicators_aifs WHERE locale = 'albury_wodonga' AND exclude IS NULL) AS included;'''.format(locale)
    curs.execute(sql)
    records = [x[0] for x in curs.fetchall()]
    print(" ({}/{} included sample points)".format(records[1],records[0]))
  else:
    print(" - {} data apparently not available ".format(locale))

print("Create table indices... "),
sql = '''
CREATE UNIQUE INDEX IF NOT EXISTS aedc_indicators_aifs_idx ON aedc_indicators_aifs USING btree ({id});
CREATE INDEX IF NOT EXISTS aedc_indicators_aifs_gix ON aedc_indicators_aifs USING GIST (geom);
CREATE UNIQUE INDEX IF NOT EXISTS aos_acara_naplan_idx ON aos_acara_naplan USING btree (aos_id, acara_school_id,locale);
CREATE UNIQUE INDEX IF NOT EXISTS aos_idx ON open_space_areas USING btree (aos_id,locale);
CREATE INDEX IF NOT EXISTS idx_aos_jsb ON open_space_areas USING gin (attributes);
'''.format(id = points_id.lower())
curs.execute(sql)
conn.commit()
print("Done.")
 
print("Create aedc match table... "),
sql = '''
CREATE TABLE IF NOT EXISTS aedc_aifs_linked AS
SELECT
  aedc.project_id, 
  latitude, 
  longitude, 
  epsg,
  linkage.*,
  aedc.geom AS aedc_geom
FROM aedc_address AS aedc
CROSS JOIN LATERAL 
  (SELECT
      ST_Distance(i.geom, aedc.geom) as match_distance_m,
      i.*
      FROM aedc_indicators_aifs i
      WHERE i.exclude IS NULL
      AND ST_DWithin(i.geom, aedc.geom, 500) 
      ORDER BY aedc.geom <-> i.geom
     LIMIT 1
   ) AS linkage;
CREATE INDEX IF NOT EXISTS aedc_participant_idx ON aedc_aifs_linked USING btree (project_id);
CREATE INDEX IF NOT EXISTS aedc_gnaf_idx ON aedc_aifs_linked USING btree ({id});
CREATE INDEX IF NOT EXISTS aedc_gnaf_gix ON aedc_aifs_linked USING GIST (geom);
'''.format(id = points_id.lower())
curs.execute(sql)
conn.commit()
 
 
 
conn.close()



