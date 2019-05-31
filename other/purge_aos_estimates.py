# Purpose: Prepare Areas of Open Space (AOS) for ntnl liveability indicators
#          - The AOS definition was modified, so we need to make sure existing estimates are removed
#          - we will then re-run.
#
# Authors:  Carl Higgs
# Date:    20180626


import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import time
import psycopg2
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Purge previous AOS estimates for re-running with modified AOS definition'

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

sql = '''
DROP TABLE IF EXISTS od_aos         ;
DROP TABLE IF EXISTS od_aos_full    ;
DROP TABLE IF EXISTS od_aos_jsonb   ;
DROP TABLE IF EXISTS od_aos_progress;
'''
curs.execute(sql)
conn.commit()

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()


