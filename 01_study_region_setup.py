# Script:  01_study_region_setup.py
# Purpose: Python set up study region boundaries
# Author:  Carl Higgs
# Date:    2018 06 05

import time
import psycopg2
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create study region boundary files in new geodatabase'

print("Import administrative boundaries for study region... "),

print("Done.")


print("Feature: {}".format(region_shape))
# If a 'region where clause' query has been defined in project file, 
#  use that query to select a subset of feature to define study region
if not pandas.np.isnan(region_where_clause):
  print("Query: {}".format(region_where_clause))
  # select subset of features to be included
  arcpy.SelectLayerByAttribute_management(in_layer_or_view  = 'feature', 
                                            selection_type    = "NEW_SELECTION", 
                                            where_clause      = "{}".format(region_where_clause))  

print("Buffer study region... "),
arcpy.env.workspace = gdb
arcpy.env.overwriteOutput = True 
if df_studyregion.loc[locale]['full_locale'] == df_studyregion.loc[locale]['region']:
  print("Study region is identical to broader region, so to avoid bug with buffering Australia, we allow that no buffer is required.  Please note that this could cause issues where the region is not a self-contained island like Australia.")
  arcpy.CopyFeatures_management(study_region,buffered_study_region)
else:
  arcpy.Buffer_analysis(study_region, buffered_study_region, study_buffer)
print("Done.")

print("Copy study region to postgis...")
command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" ' \
        + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
        + 'user={user} password = {pwd}" '.format(user = db_user,pwd = db_pwd) \
        + '{gdb} "{feature}" '.format(gdb = gdb_path,feature = study_region) \
        + '-lco geometry_name="geom"'
## Note  --- on Carl's computer, he had to specify a specific dir to get 
## correct instance of ogr2ogr; this wasn't req'd on Bec's computer I think
## and that specific dir doesn't exist for her, so I have commented out this line
## and replaced with a simpler line which hopefully will work for others
# sp.call(command, shell=True, cwd='C:/OSGeo4W64/bin/')
sp.call(command, shell=True)

print("Copy buffered study region to postgis...")
command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" ' \
        + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
        + 'user={user} password = {pwd}" '.format(user = db_user,pwd = db_pwd) \
        + '{gdb} "{feature}" '.format(gdb = gdb_path,feature = buffered_study_region) \
        + '-lco geometry_name="geom"'
sp.call(command, shell=True)

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd, host = db_host,  port = db_port)
curs = conn.cursor()
curs.execute(grant_query)
conn.commit()

# output to completion log					
script_running_log(script, task, start, locale)
conn.close()
