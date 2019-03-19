# Purpose: Purge childcare destinations from database
#
# Authors:  Carl Higgs
# Date:    20190319


import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import time
import psycopg2
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])

category = 'childcare'
string     = 'childcare_%'

task = 'Purge {} destinations from dest_type, od_counts, od_closest, od_distances_3200m tables'.format(category)

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

sql = '''
DELETE FROM dest_type WHERE dest_class LIKE '{string}';
DELETE FROM od_counts WHERE dest_class LIKE '{string}';
DELETE FROM od_closest WHERE dest_class LIKE '{string}';
DELETE FROM od_distances_3200m WHERE dest_class LIKE '{string}';
'''.format(string = string)
curs.execute(sql)
conn.commit()

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()


