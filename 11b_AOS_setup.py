# Purpose: Prepare Areas of Open Space (AOS) for ntnl liveability indicators
#           -- *** Assumes already in correct projection for project (e.g. GDA2020 GA LCC) *** 
#           -- copies features within study region to project gdb
#           -- calculates geodesic area in hectares
#           -- makes temporary line feature from polygons
#           -- traces vertices at set interval (pos_vertices in config file)
#
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
task = 'Prepare Areas of Open Space (AOS)'


# osm to pgsql
print("Copy OSM excerpt to pgsql")
command = 'osm2pgsql -U {user} -l -d {db} {osm} --hstore --style {style} --prefix {prefix}'.format(user = db_user, 
                                                                                 db = db,
                                                                                 osm = osm,
                                                                                 style = osm2pgsql_style,
                                                                                 prefix = osm_date) 
sp.call(command, shell=True, cwd=osm2pgsql_exe)                           

  
# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()  
  
# gdb to pgsql
print("Copy the pos shape to PostgreSQL database..."),
command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" ' \
        + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
        + 'user={user} password = {pwd} " '.format(user = db_user,pwd = db_pwd) \
        + '{gdb} "{feature}" '.format(gdb = gdb_path,feature = "pos_shape") \
        + '-lco geometry_name="geom" '
sp.call(command, shell=True)

# Depending on whether pos_category variable is defined, this will be included in the
# pos_50m_vertices table; if included, the pos_category variable can be used to define 
# queries in the config file.
print("Copy the pos points with area attribute data to PostgreSQL database..."),
command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" ' \
        + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
        + 'user={user} password = {pwd} " '.format(user = db_user,pwd = db_pwd) \
        + '{gdb} "{feature}" '.format(gdb = gdb_path,feature = "pos_50m_vertices") \
        + '-lco geometry_name="geom" '
sp.call(command, shell=True)
curs.execute(grant_query)
conn.commit()
  
conn.close()
print(" Done.") 
 
# output to completion log    
script_running_log(script, task, start, locale)
