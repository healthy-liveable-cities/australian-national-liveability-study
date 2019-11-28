# Script:  18_parcel_exclusion.py
# Purpose: This script develops a list of suspect parcels to investigate and exclude.
# Author:  Carl Higgs

import os
import sys
import time
import psycopg2
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = "Create additional Mildura specific exclusions of address points not in area of interest"

locale = 'mildura'
db = 'li_mildura_2018'

insert = "INSERT INTO excluded_parcels SELECT a.{id},a.geom, ".format(id = points_id.lower())
table = "\nFROM parcel_dwellings AS a \nLEFT JOIN "
match = " AS b \n USING({id})  \nWHERE ".format(id = points_id.lower())
null = "IS NULL ON CONFLICT ({id},indicator) DO NOTHING ".format(id = points_id.lower())

# exclude on null indicator, and on null distance
query = '''
{insert} 'not in area of interest' {table} study_parcels {match} b.{id} {null};
'''.format(insert = insert, table = table, match = match, island_exception = island_exception, null = null, id = points_id.lower())

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

curs.execute(query)
conn.commit()


# output to completion log    
script_running_log(script, task, start, locale)

# clean up
conn.close()