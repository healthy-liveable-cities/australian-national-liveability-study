# Script:  aedc_national_collation.py
# Purpose: Collate study region indicator output sql dumps
# Author:  Carl Higgs 
# Date:    20190703
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
year = 2018
aedc_dir = os.path.join(folderPath,'study_region','ntnl_li_inds')
print("This script assumes the database {db} has been created!\n".format(db = db))
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

curs.execute('''SELECT 1 WHERE to_regclass('public.parcel_indicators') IS NOT NULL;''')
res = curs.fetchone()
if res:
    print("AEDC AIFS table already exists.\n")
    sql = '''SELECT DISTINCT(locale) FROM parcel_indicators ORDER BY locale;'''
    curs.execute(sql)
    processed_locales = [x[0] for x in curs.fetchall()]
else:
    print("Create empty tables for parcel indicators... ")
    command = 'psql li_australia_2018 < ntnl_li_inds_schema.sql'
    sp.call(command, shell=True,cwd=aedc_dir)   
    processed_locales = []
    print("Done.\n")

print("Looping over study regions and importing data if available and not previously processed...")
locale_field_length = 7 + len(max(study_regions,key=len))
for locale in sorted(study_regions, key=str.lower):
  sql = 'ntnl_li_inds_{}_{}_Fc.sql'.format(locale,year)
  if locale in processed_locales:
    print((" - {:"+str(locale_field_length)+"}: previously processed").format(locale))
  elif os.path.isfile(os.path.join(aedc_dir,sql)):
    print((" - {:"+str(locale_field_length)+"}: processing now... ").format(locale)),
    command = 'pg_restore -a -Fc -d li_australia_2018 < {}'.format(sql)
    sp.call(command, shell=True,cwd=aedc_dir)   
    print("Done!")
  else:
    print((" - {:"+str(locale_field_length)+"}: data apparently not available ").format(locale))

print("Create table indices... "),
sql = '''
ALTER TABLE ONLY parcel_indicators 
  ADD CONSTRAINT pkey_parcel_indicators PRIMARY KEY (gnaf_pid);
CREATE INDEX gix_parcel_indicators ON parcel_indicators USING gist (geom);

ALTER TABLE ONLY dest_closest_indicators 
  ADD CONSTRAINT pkey_dest_closest_indicators PRIMARY KEY (gnaf_pid);
CREATE INDEX gix_dest_closest_indicators ON dest_closest_indicators USING gist (geom);

ALTER TABLE ONLY dest_array_indicators 
  ADD CONSTRAINT pkey_dest_array_indicators PRIMARY KEY (gnaf_pid);
CREATE INDEX gix_dest_array_indicators ON dest_array_indicators USING gist (geom);

ALTER TABLE ONLY od_aos_jsonb 
  ADD CONSTRAINT pkey_od_aos_jsonb PRIMARY KEY (gnaf_pid);
CREATE INDEX ix_od_aos_jsonb ON od_aos_jsonb USING btree (gnaf_pid);
CREATE INDEX ix_od_aos_jsonb_aos_id ON od_aos_jsonb USING btree (((attributes -> 'aos_id'::text)));
CREATE INDEX ix_od_aos_jsonb_distance ON od_aos_jsonb USING btree (((attributes -> 'distance'::text)));

ALTER TABLE ONLY open_space_areas 
  ADD CONSTRAINT pkey_open_space_areas PRIMARY KEY (gnaf_pid);
CREATE INDEX gix_open_space_areas ON open_space_areas USING gist (geom);
CREATE INDEX ginx_aos_jsb ON open_space_areas USING gin (attributes);

CREATE INDEX ix_ind_summary_indicators ON ind_summary USING btree (indicators);
'''.format(id = points_id.lower())
curs.execute(sql)
conn.commit()
print("Done.\n")

 
print("Create aedc match table... "),
sql = '''
DROP TABLE IF EXISTS aedc_aifs_linked;
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
print("Done.\n")



# output to completion log    
script_running_log(script, task, start, locale)
conn.close()


