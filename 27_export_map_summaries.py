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
date = '20200212'
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))


if locale!='australia':
    out_dir = 'D:/ntnl_li_2018_template/data/study_region/_exports'
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    out_file = 'li_{}_{}_{}_Fc.sql'.format(locale,year,date)
    print("Creating sql dump to: {}".format(os.path.join(out_dir,out_file))),
    command = (
               'pg_dump -U {db_user} -h localhost -Fc  '
               '-t "li_inds_lga_dwelling" -t "li_inds_lga_person" -t "li_inds_mb_dwelling" '
               '-t "li_inds_mb_person" -t "li_inds_region_dwelling" -t "li_inds_region_person" '
               '-t "li_inds_sa1_dwelling" -t "li_inds_sa1_person" -t "li_inds_sa2_dwelling" '
               '-t "li_inds_sa2_person" -t "li_inds_sa3_dwelling" -t "li_inds_sa3_person" '
               '-t "li_inds_sa4_dwelling" -t "li_inds_sa4_person" -t "li_inds_sos_dwelling" '
               '-t "li_inds_sos_person" -t "li_inds_ssc_dwelling" -t "li_inds_ssc_person" '
               '{db} > {out_file}'
               ).format(db = db,db_user = db_user,out_file=out_file)  
    sp.call(command, shell=True,cwd=out_dir)   
    print("Done.")

# Create table schema definition using Albury Wodonga:
if locale=='albury_wodonga':
    schema = 'li_inds_schema_{}_{}_{}.sql'.format(locale,year,date)
    print("Creating sql dump to: {}".format(os.path.join(out_dir,schema))),
    command = (
               'pg_dump -U {db_user} -h localhost --schema-only '
               '-t "li_inds_lga_dwelling" -t "li_inds_lga_person" -t "li_inds_mb_dwelling" '
               '-t "li_inds_mb_person" -t "li_inds_region_dwelling" -t "li_inds_region_person" '
               '-t "li_inds_sa1_dwelling" -t "li_inds_sa1_person" -t "li_inds_sa2_dwelling" '
               '-t "li_inds_sa2_person" -t "li_inds_sa3_dwelling" -t "li_inds_sa3_person" '
               '-t "li_inds_sa4_dwelling" -t "li_inds_sa4_person" -t "li_inds_sos_dwelling" '
               '-t "li_inds_sos_person" -t "li_inds_ssc_dwelling" -t "li_inds_ssc_person" '
               '{db} > {schema}'
               ).format(db = db,db_user = db_user,schema=schema)    
    sp.call(command, shell=True,cwd=out_dir)   
    print("Done.")

if locale=='australia':
    schema = 'li_inds_schema_{}.sql'.format(date)
    # Connect to postgresql database     
    db = 'li_australia_2018'
    year = 2018
    exports_dir = os.path.join(folderPath,'study_region','_exports')
    print("This script assumes the database {db} has been created!\n".format(db = db))
    conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
    curs = conn.cursor()
    sql = '''
    DROP TABLE IF EXISTS li_inds_lga_dwelling    ;
    DROP TABLE IF EXISTS li_inds_lga_person      ;
    DROP TABLE IF EXISTS li_inds_mb_dwelling     ;
    DROP TABLE IF EXISTS li_inds_mb_person       ;
    DROP TABLE IF EXISTS li_inds_region_dwelling ;
    DROP TABLE IF EXISTS li_inds_region_person   ;
    DROP TABLE IF EXISTS li_inds_sa1_dwelling    ;
    DROP TABLE IF EXISTS li_inds_sa1_person      ;
    DROP TABLE IF EXISTS li_inds_sa2_dwelling    ;
    DROP TABLE IF EXISTS li_inds_sa2_person      ;
    DROP TABLE IF EXISTS li_inds_sa3_dwelling    ;
    DROP TABLE IF EXISTS li_inds_sa3_person      ;
    DROP TABLE IF EXISTS li_inds_sa4_dwelling    ;
    DROP TABLE IF EXISTS li_inds_sa4_person      ;
    DROP TABLE IF EXISTS li_inds_sos_dwelling    ;
    DROP TABLE IF EXISTS li_inds_sos_person      ;
    DROP TABLE IF EXISTS li_inds_ssc_dwelling    ;
    DROP TABLE IF EXISTS li_inds_ssc_person      ;
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
      sql = 'li_{}_{}_{}_Fc.sql'.format(locale,year,date)
      if locale in processed_locales:
        print((" - {:"+str(locale_field_length)+"}: previously processed").format(locale))
      elif os.path.isfile(os.path.join(exports_dir,sql)):
        print((" - {:"+str(locale_field_length)+"}: processing now... ").format(locale)),
        command = 'pg_restore -a -Fc -d li_australia_2018 < {}'.format(sql)
        sp.call(command, shell=True,cwd=exports_dir)   
        print("Done!")
      else:
        print((" - {:"+str(locale_field_length)+"}: data apparently not available ").format(locale))


print("All done!")
# output to completion log    
script_running_log(script, task, start)
