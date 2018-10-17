# Import OSM to a postgresql 
# Author:  Carl Higgs
# Date:    20180626


import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import arcpy
import time
import psycopg2
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Import OSM'

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

# import buffered study region OSM excerpt to pgsql, 
print("Copying OSM excerpt to pgsql..."),
command = 'osm2pgsql -U {user} -l -d {db} {osm} --hstore --style {style} --prefix {prefix}'.format(user = db_user, 
                                                                               db = db,
                                                                               osm = osm_source,
                                                                               style = osm2pgsql_style,
                                                                               prefix = osm_prefix) 
sp.call(command, shell=True, cwd=osm2pgsql_exe)                           
print("Done.")

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
curs.execute(grant_query)
conn.commit()

