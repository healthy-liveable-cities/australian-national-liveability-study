# Purpose: Create liveability score cards
# Author:  Carl Higgs 
# Date:    2020-01-13

import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine
from datetime import datetime

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Export liveability indicator region estimates'

date = datetime.today().strftime('%Y%m%d')
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))


if locale!='australia':
    sql = '''
          -- Ensure open space areas are indexed by both aos_id and locale
          DROP INDEX IF EXISTS aos_idx;
          CREATE INDEX IF NOT EXISTS open_space_areas_idx ON open_space_areas (aos_id,locale);
          DROP INDEX IF EXISTS "od_pt_800m_cl_distance";  -- fixing up incorrect index
          CREATE INDEX IF NOT EXISTS "od_pt_800m_cl_distance" ON od_pt_800m_cl ((attributes->'distance'));
          CREATE INDEX IF NOT EXISTS "od_aos_jsonb_locale_idx" ON od_aos_jsonb (locale);
          CREATE INDEX IF NOT EXISTS "dest_closest_indicators_locale_idx" ON dest_closest_indicators (locale);
          CREATE INDEX IF NOT EXISTS "parcel_indicators_locale_idx" ON parcel_indicators (locale);
          '''
    engine.execute(sql)
    out_dir = 'D:/ntnl_li_2018_template/data/study_region/_exports'
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    out_file = 'li_parcel_{}_{}_{}_Fc.sql'.format(locale,year,date)
    print("Creating sql dump to: {}".format(os.path.join(out_dir,out_file))),
    command = (
               'pg_dump -U {db_user} -h localhost -Fc  '
               ' -t "parcel_indicators" '
               ' -t "dest_closest_indicators" '
               ' -t "open_space_areas" '
               ' -t "od_aos_jsonb" '
               ' -t "ind_pt_2019_distance_800m_cl" '
               ' -t "ind_pt_2019_headway_800m" '
               '{db} > {out_file}'
               ).format(db = db,db_user = db_user,out_file=out_file)  
    sp.call(command, shell=True,cwd=out_dir)   
    print("Done.")

# Create table schema definition using Albury Wodonga:
if locale=='albury_wodonga':
    schema = 'li_parcel_schema_{}.sql'.format(date)
    print("Creating sql dump to: {}".format(os.path.join(out_dir,schema))),
    command = (
               'pg_dump -U {db_user} -h localhost --schema-only '
               ' -t "parcel_indicators" '
               ' -t "dest_closest_indicators" '
               ' -t "open_space_areas" '
               ' -t "od_aos_jsonb" '
               ' -t "ind_pt_2019_distance_800m_cl" '
               ' -t "ind_pt_2019_headway_800m" '
               '{db} > {schema}'
               ).format(db = db,db_user = db_user,schema=schema)    
    sp.call(command, shell=True,cwd=out_dir)   
    print("Done.")

if locale=='australia':
    if len(sys.argv) >= 3:
        date = sys.argv[2]
    schema = 'li_parcel_schema_{}.sql'.format(date)
    # Connect to postgresql database     
    db = 'li_australia_2018'
    year = 2018
    exports_dir = os.path.join(folderPath,'study_region','_exports')
    print("This script assumes the database {db} has been created!\n".format(db = db))
    conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
    curs = conn.cursor()
    sql = '''
    DROP TABLE IF EXISTS "parcel_indicators" ;
    DROP TABLE IF EXISTS "dest_closest_indicators" ;
    DROP TABLE IF EXISTS "open_space_areas" ;
    DROP TABLE IF EXISTS "od_aos_jsonb" ;
    DROP TABLE IF EXISTS "ind_pt_2019_distance_800m_cl" ;
    DROP TABLE IF EXISTS "ind_pt_2019_headway_800m" ;
    DROP INDEX IF EXISTS idx_aos_jsb;
    DROP INDEX IF EXISTS aos_idx;
    -- Also drop redundant for now tables
    DROP TABLE IF EXISTS "dest_array_indicators" ;
    DROP TABLE IF EXISTS "edges" ;
    DROP TABLE IF EXISTS "nodes" ;
    DROP TABLE IF EXISTS "uli_inds" ;
    '''
    curs.execute(sql)
    conn.commit()

    print("Create empty tables for parcel indicators... ")
    command = 'psql li_australia_2018 < {}'.format(schema)
    print(command)
    sp.call(command, shell=True,cwd=exports_dir)   

    # curs.execute('''SELECT study_region FROM li_inds_region_dwelling;''')
    # processed_locales = [x[0] for x in curs.fetchall()]
    processed_locales = []
    print("Done.\n")

    print("Looping over study regions and importing data if available and not previously processed...")
    locale_field_length = 7 + len(max(study_regions,key=len))
    for locale in sorted(study_regions, key=str.lower):
      sql = 'li_parcel_{}_{}_{}_Fc.sql'.format(locale,year,date)
      if locale in processed_locales:
        print((" - {:"+str(locale_field_length)+"}: previously processed").format(locale))
      elif os.path.isfile(os.path.join(exports_dir,sql)):
        print((" - {:"+str(locale_field_length)+"}: processing now... ").format(locale)),
        command = 'pg_restore -a -Fc -d li_australia_2018 < {}'.format(sql)        
        # command = (
                   # 'pg_restore -a -Fc '
                   # ' -t "parcel_indicators" '
                   # ' -t "dest_closest_indicators" '
                   # ' -t "open_space_areas" '
                   # ' -t "od_aos_jsonb" '
                   # ' -t "ind_pt_2019_distance_800m_cl" '
                   # ' -t "ind_pt_2019_headway_800m" '
                   # ' -d li_australia_2018 < {sql}'
                   # ).format(sql=sql)    
        sp.call(command, shell=True,cwd=exports_dir)   
        print("Done!")
      else:
        print((" - {:"+str(locale_field_length)+"}: data apparently not available ").format(locale))

print("All done!")
# output to completion log    
script_running_log(script, task, start)
