# Purpose: Evaluate Euclidean buffer co-location of Areas of Open Space with other amenities; in particular
#          -- cafes and restaurants @ 100m
#          -- lighting @ 0m?  (subject to data availability!)
#                   - check osm tag in first instance
#                   - then check external data sources
#          -- toilets @ 100m
# Author:  Carl Higgs
# Date:    20180626


# import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
# import arcpy
# import time
# import psycopg2
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Co-locate Areas of Open Space (AOS) with other amenities'

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
# conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
# curs = conn.cursor()  

print('''This is a place holder scripts for Areas of Open Space (AOS) co-location.
#
# Purpose: Evaluate Euclidean buffer co-location of Areas of Open Space with other amenities; in particular
#          -- cafes and restaurants @ 100m
#          -- lighting @ 0m?  (subject to data availability!)
#                   - check osm tag in first instance
#                   - then check external data sources
#          -- toilets @ 100m

So, yet to be written - watch this space...
'''