# Script:  26_area_dwelling_densities.py
# Purpose: Calculate area level net and gross density measures
# Author:  Carl Higgs
# Date:    25 October 2019


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
task = 'create destination indicator tables'

#  Connect to database
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

# sql = '''

# '''
# curs.execute(sql)

print("This script is To Do")

# output to completion log
script_running_log(script, task, start)
conn.close()
