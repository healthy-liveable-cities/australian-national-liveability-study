# Script:  17_aedc_indicators_aifs.py
# Purpose: Create aedc indicators for AIFS (condensed form)
# Author:  Carl Higgs 
# Date:    20180717

import time
import psycopg2 
import numpy as np
import pandas
import os
import sys
from sqlalchemy import create_engine
import subprocess as sp
from script_running_log import script_running_log

date_time = time.strftime("%Y%m%d-%H%M")

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

out_dir = os.path.join(folderPath,'study_region','ntnl_li_inds')
if not os.path.exists(out_dir):
        os.makedirs(out_dir)
os.environ['PGPASSWORD'] = db_pwd

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create schema for national liveability indicators, based on tables for {}'.format(full_locale)

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                            pwd  = db_pwd,
                                                            host = db_host,
                                                            db   = db))
sql = '''
ALTER TABLE od_aos_jsonb ADD COLUMN IF NOT EXISTS locale text;
UPDATE od_aos_jsonb SET locale = '{locale}';
'''.format(locale = locale)
curs.execute(sql)
conn.commit()

# Note - i generated the create table commands with the following dump applied to Albury Wodonga:
out_file = 'ntnl_li_inds_schema.sql'.format(db)
print("\tCreating sql dump to: {}".format(os.path.join(out_dir,out_file))),
command = 'pg_dump -U {db_user} -h localhost --schema-only -t "parcel_indicators" -t "dest_closest_indicators" -t "dest_array_indicators" -t "od_aos_jsonb" -t "open_space_areas" -t "ind_summary" -t "exclusion_summary" {db} > {out_file}'.format(db = db,db_user = db_user,out_file=out_file)    
sp.call(command, shell=True,cwd=out_dir)   
print("Done.")

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()

