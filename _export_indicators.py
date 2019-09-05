# Script:  _export_indicators.py
# Purpose: Export indicators for national collation
# Author:  Carl Higgs 
# Date:    3 September 2019

import os
import sys
import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create area level indicator tables for Disability and Health ({})'.format(locale)
print(task)
# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db))


if locale!='australia':
    sql = '''
    ALTER TABLE open_space_areas ADD COLUMN IF NOT EXISTS locale text;
    UPDATE open_space_areas SET locale = '{locale}';
    '''.format(locale = locale)
    curs.execute(sql)
    conn.commit()
    out_dir = 'D:/ntnl_li_2018_template/data/studyregion/ntnl_li_inds'
    out_file = 'ntnl_li_inds_{}_{}_Fc.sql'.format(locale,year)
    print("Creating sql dump to: {}".format(os.path.join(out_dir,out_file))),
    command = 'pg_dump -U {db_user} -h localhost -Fc -t "parcel_indicators" -t "dest_closest_indicators" -t "dest_array_indicators" -t "od_aos_jsonb" -t "open_space_areas" -t "ind_summary" -t "exclusion_summary" -t "area_indicators_mb_json" -t "area_linkage" {db} > {out_file}'.format(db = db,db_user = db_user,out_file=out_file)    
    sp.call(command, shell=True,cwd=out_dir)   
    print("Done.")

# output to completion log    
script_running_log(script, task, start, locale)
