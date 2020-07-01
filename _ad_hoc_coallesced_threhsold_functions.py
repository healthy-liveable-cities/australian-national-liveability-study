
#import packages
import os
import sys
import time
import numpy as np
import psycopg2 
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create coalesced threhsold functions (null automatically to zero score)'

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

sql = '''
 CREATE OR REPLACE FUNCTION threshold_coalesce_hard(in int, in int, out int) 
    AS $$ COALESCE(SELECT ($1 < $2)::int,0) $$
    LANGUAGE SQL;

CREATE OR REPLACE FUNCTION threshold_coalesce_soft(in int, in int, out double precision) 
    AS $$ SELECT COALESCE(1 - 1/(1+exp(-5*($1-$2)/($2::float))),0) $$
    LANGUAGE SQL;    
'''
engine.execute(sql)
engine.dispose()

# output to completion log
script_running_log(script, task, start)
conn.close()
