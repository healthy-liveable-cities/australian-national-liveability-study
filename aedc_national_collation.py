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

print("Create empty AEDC AIFS tables... ")
command = 'psql li_australia_2018 < aedc_aifs_schema.sql'
sp.call(command, shell=True,cwd=aedc_dir)   
print("Done.")

print("Looping over study regions to check if required output exists in their folder; if it does, its imported...")
for locale in study_regions:
  sql = 'aedc_aifs_li_{}_2018_Fc.sql'.format(locale)
  if os.path.isfile(os.path.join(aedc_dir,sql)):
    print(" - {}".format(locale))
    command = 'pg_restore -a -Fc -d li_australia_2018 < {}'.format(sql)
    sp.call(command, shell=True,cwd=aedc_dir)   

print("Create table indices... "),
sql = '''
CREATE UNIQUE INDEX IF NOT EXISTS aedc_indicators_aifs_idx ON aedc_indicators_aifs USING btree ({id});
CREATE UNIQUE INDEX IF NOT EXISTS aedc_indicators_aifs_gix ON aedc_indicators_aifs USING GIST (geom);
CREATE UNIQUE INDEX IF NOT EXISTS aos_acara_naplan_idx ON aos_acara_naplan USING btree (aos_id, acara_school_id,locale);
CREATE UNIQUE INDEX IF NOT EXISTS aos_idx ON open_space_areas USING btree (aos_id,locale);
CREATE INDEX IF NOT EXISTS idx_aos_jsb ON open_space_areas USING gin (attributes);
'''.format(id = points_id.lower())
curs.execute(sql)
conn.commit()
print("Done.")
 
print("Create aedc match table... "),
sql = '''
CREATE TABLE IF NOT EXISTS aedc_gnaf AS
(SELECT aedc.project_id, lon, lat, p.{id},ST_Distance(aedc.geom,p.geom) AS match_distance_m,  aedc.geom
FROM aedc_address AS aedc
CROSS JOIN LATERAL 
(SELECT {id},  geom FROM aedc_indicators_aifs AS i
  ORDER BY aedc.geom <-> i.geom LIMIT 1) AS p
WHERE ST_Distance(aedc.geom,p.geom) < 500 );
CREATE INDEX IF NOT EXISTS aedc_participant_idx ON aedc_gnaf USING btree (project_id);
CREATE INDEX IF NOT EXISTS aedc_gnaf_idx ON aedc_gnaf USING btree ({id});
CREATE INDEX IF NOT EXISTS aedc_gnaf_gix ON aedc_gnaf USING GIST (geom);
'''.format(id = points_id.lower())
curs.execute(sql)
conn.commit()
 
 
 
conn.close()



