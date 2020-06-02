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
from _project_setup import *

out_dir = os.path.join(folderPath,'study_region','_exports')
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

# Note - i generated the create table commands with the following dump applied to Albury Wodonga:
out_file = 'score_cards_schema.sql'.format(db)
print("Creating sql dump to: {}".format(os.path.join(out_dir,out_file))),
command = (
           'pg_dump -U {db_user} -h localhost --schema-only '
           '-t "score_card_lga_dwelling" -t "score_card_lga_person" -t "score_card_mb_dwelling" '
           '-t "score_card_mb_person" -t "score_card_region_dwelling" -t "score_card_region_person" '
           '-t "score_card_sa1_dwelling" -t "score_card_sa1_person" -t "score_card_sa2_dwelling" '
           '-t "score_card_sa2_person" -t "score_card_sa3_dwelling" -t "score_card_sa3_person" '
           '-t "score_card_sa4_dwelling" -t "score_card_sa4_person" -t "score_card_sos_dwelling" '
           '-t "score_card_sos_person" -t "score_card_ssc_dwelling" -t "score_card_ssc_person" '
           '-t "ind_score_card" '
           '{db} > {out_file}'
           ).format(db = db,db_user = db_user,out_file=out_file)    
sp.call(command, shell=True,cwd=out_dir)   
print("Done.")

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()

