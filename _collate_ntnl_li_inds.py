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
ntnl_li_dir = os.path.join(folderPath,'study_region','ntnl_li_inds')
print("This script assumes the database {db} has been created!\n".format(db = db))
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

curs.execute('''SELECT 1 WHERE to_regclass('public.parcel_indicators') IS NOT NULL;''')
res = curs.fetchone()
if res:
    print("Parcel indicators table already exists.  Dropping existing indices, and checking previously processed results\n")
    sql = '''
    DROP INDEX IF EXISTS ix_od_aos_jsonb_aos_id;
    DROP INDEX IF EXISTS ix_od_aos_jsonb_distance;
    DROP INDEX IF EXISTS ix_od_aos_jsonb;
    DROP INDEX IF EXISTS ix_ind_summary_indicators;
    DROP INDEX IF EXISTS gix_area_linkage;
    DROP INDEX IF EXISTS gix_parcel_indicators;
    DROP INDEX IF EXISTS gix_dest_closest_indicators;
    DROP INDEX IF EXISTS gix_dest_array_indicators;
    DROP INDEX IF EXISTS gix_open_space_areas;
    DROP INDEX IF EXISTS gix_area_indicators_mb_json;
    DROP INDEX IF EXISTS gix_li_inds_sa1_dwelling;
    DROP INDEX IF EXISTS ginx_aos_jsb;
    ALTER TABLE area_linkage DROP CONSTRAINT IF EXISTS pkey_area_linkage;
    ALTER TABLE parcel_indicators DROP CONSTRAINT IF EXISTS pkey_parcel_indicators;
    ALTER TABLE dest_closest_indicators DROP CONSTRAINT IF EXISTS pkey_dest_closest_indicators;
    ALTER TABLE dest_array_indicators  DROP CONSTRAINT IF EXISTS pkey_dest_array_indicators;
    ALTER TABLE od_aos_jsonb DROP CONSTRAINT IF EXISTS pkey_od_aos_jsonb;
    ALTER TABLE open_space_areas DROP CONSTRAINT IF EXISTS pkey_open_space_areas;
    ALTER TABLE area_indicators_mb_json DROP CONSTRAINT IF EXISTS pkey_area_indicators_mb_json;
    ALTER TABLE li_inds_sa1_dwelling DROP CONSTRAINT IF EXISTS pkey_li_inds_sa1_dwelling;
    '''
    curs.execute(sql)
    conn.commit()
    sql = '''
    SELECT DISTINCT(locale) FROM parcel_indicators ORDER BY locale;
    '''
    curs.execute(sql)
    processed_locales = [x[0] for x in curs.fetchall()]
else:
    print("Create empty tables for parcel indicators... ")
    command = 'psql li_australia_2018 < ntnl_li_inds_schema.sql'
    sp.call(command, shell=True,cwd=ntnl_li_dir)   
    processed_locales = []
    print("Done.\n")

print("Looping over study regions and importing data if available and not previously processed...")
locale_field_length = 7 + len(max(study_regions,key=len))
for locale in sorted(study_regions, key=str.lower):
  sql = 'ntnl_li_inds_{}_{}_Fc.sql'.format(locale,year)
  if locale in processed_locales:
    print((" - {:"+str(locale_field_length)+"}: previously processed").format(locale))
  elif os.path.isfile(os.path.join(ntnl_li_dir,sql)):
    print((" - {:"+str(locale_field_length)+"}: processing now... ").format(locale)),
    command = 'pg_restore -a -Fc -d li_australia_2018 < {}'.format(sql)
    sp.call(command, shell=True,cwd=ntnl_li_dir)   
    print("Done!")
  else:
    print((" - {:"+str(locale_field_length)+"}: data apparently not available ").format(locale))

print("Create table indices... "),
sql = '''
ALTER TABLE ONLY area_linkage ADD CONSTRAINT pkey_area_linkage PRIMARY KEY (mb_code_2016);
CREATE INDEX gix_area_linkage ON area_linkage USING gist (geom);

ALTER TABLE ONLY parcel_indicators ADD CONSTRAINT pkey_parcel_indicators PRIMARY KEY (gnaf_pid);
CREATE INDEX gix_parcel_indicators ON parcel_indicators USING gist (geom);

ALTER TABLE ONLY dest_closest_indicators ADD CONSTRAINT pkey_dest_closest_indicators PRIMARY KEY (gnaf_pid);
CREATE INDEX gix_dest_closest_indicators ON dest_closest_indicators USING gist (geom);

ALTER TABLE ONLY dest_array_indicators  ADD CONSTRAINT pkey_dest_array_indicators PRIMARY KEY (gnaf_pid);
CREATE INDEX gix_dest_array_indicators ON dest_array_indicators USING gist (geom);

ALTER TABLE ONLY od_aos_jsonb ADD CONSTRAINT pkey_od_aos_jsonb PRIMARY KEY (gnaf_pid);
CREATE INDEX ix_od_aos_jsonb ON od_aos_jsonb USING btree (gnaf_pid);
CREATE INDEX ix_od_aos_jsonb_aos_id ON od_aos_jsonb USING btree (((attributes -> 'aos_id'::text)));
CREATE INDEX ix_od_aos_jsonb_distance ON od_aos_jsonb USING btree (((attributes -> 'distance'::text)));

ALTER TABLE ONLY open_space_areas ADD CONSTRAINT pkey_open_space_areas PRIMARY KEY (aos_id,locale);
CREATE INDEX gix_open_space_areas ON open_space_areas USING gist (geom);
CREATE INDEX ginx_aos_jsb ON open_space_areas USING gin (attributes);

CREATE INDEX ix_ind_summary_indicators ON ind_summary USING btree (indicators);

ALTER TABLE ONLY area_indicators_mb_json ADD CONSTRAINT pkey_area_indicators_mb_json PRIMARY KEY (mb_code_2016);
CREATE INDEX gix_area_indicators_mb_json ON area_indicators_mb_json USING gist (geom);

ALTER TABLE ONLY li_inds_sa1_dwelling ADD CONSTRAINT pkey_li_inds_sa1_dwelling PRIMARY KEY (sa1_maincode_2016);
CREATE INDEX gix_li_inds_sa1_dwelling ON li_inds_sa1_dwelling USING gist (geom);
'''.format(id = points_id.lower())
curs.execute(sql)
conn.commit()
print("Done.\n")

 # output to completion log    
script_running_log(script, task, start, locale)
conn.close()


