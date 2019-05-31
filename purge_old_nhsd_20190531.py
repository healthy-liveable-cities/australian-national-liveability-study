import os
import time
import sys
import psycopg2 
import numpy as np
from progressor import progressor

from script_running_log import script_running_log

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Purge results relating to old incomplete NHSD dataset from database'

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

df_nhsd = pandas.read_excel(xls, 'nhsd classification',index_col=0)

old_names = "'{}'".format("','".join(df_nhsd['old name'].values))

sql = '''
DELETE FROM od_closest WHERE dest_name IN ({old_names});
DELETE FROM log_od_distances WHERE dest_name IN ({old_names});
DELETE FROM od_distances_3200m WHERE dest_class IN ({old_names});
'''.format(old_names=old_names)
print(sql)
curs.execute(sql)
conn.commit()

for name in df_nhsd['old name'].values:
    sql = '''
    ALTER TABLE dest_distance_m DROP COLUMN IF EXISTS "{name}";
    ALTER TABLE dest_distances_3200m DROP COLUMN IF EXISTS "{name}";
    '''.format(name = name)
    print(sql)
    curs.execute(sql)
    conn.commit()

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
