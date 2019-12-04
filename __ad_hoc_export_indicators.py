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
from datetime import datetime

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Export indicators for national collation ({})'.format(locale)
print(task)

date = datetime.today().strftime('%Y%m%d')

# Connect to postgresql database     
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
    out_file = 'score_card_{}_{}_{}_Fc.sql'.format(locale,year,date)
    print("Creating sql dump to: {}".format(os.path.join(out_dir,out_file))),
    command = (
               'pg_dump -U {db_user} -h localhost -Fc  '
               '-t "score_card_lga_dwelling" -t "score_card_lga_person" -t "score_card_mb_dwelling" '
               '-t "score_card_mb_person" -t "score_card_region_dwelling" -t "score_card_region_person" '
               '-t "score_card_sa1_dwelling" -t "score_card_sa1_person" -t "score_card_sa2_dwelling" '
               '-t "score_card_sa2_person" -t "score_card_sa3_dwelling" -t "score_card_sa3_person" '
               '-t "score_card_sa4_dwelling" -t "score_card_sa4_person" -t "score_card_sos_dwelling" '
               '-t "score_card_sos_person" -t "score_card_ssc_dwelling" -t "score_card_ssc_person" '
               '{db} > {out_file}'
               ).format(db = db,db_user = db_user,out_file=out_file)  
    sp.call(command, shell=True,cwd=out_dir)   
    print("Done.")

# output to completion log    
script_running_log(script, task, start, locale)
