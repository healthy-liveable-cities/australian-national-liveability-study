# Purpose: Make Hex grid corresponding to polygon feature
# Author:  Carl Higgs
# Date:    2017 03 10
#
# Note: Uses the 'Create Hexagon Tesselation' geoprocessing package
#       Author: Tim Whiteaker
#       http://www.arcgis.com/home/item.html?id=03388990d3274160afe240ac54763e57
#       freely available under the Berkeley Software Distribution license.
#       The toolbox should be located in folder with this script
#
#       It assumes that the units of analysis are in metres.

import arcpy
import time
import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start  = time.time()
script = os.path.basename(sys.argv[0])

# ArcGIS environment settings
arcpy.env.workspace = folderPath  
# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(os.path.join(temp,db)):
    os.makedirs(os.path.join(temp,db))
    
arcpy.env.scratchWorkspace = os.path.join(temp,db)  
arcpy.env.overwriteOutput = True 

# SQL Settings 
arcpy.ImportToolbox(os.path.join(folderPath,create_hexagon_tbx))

# OUTPUT PROCESS
task1 = 'make {} km diagonal hex grid of feature {} to output feature {}'.format((float(hex_diag)*.001),buffered_study_region,hex_grid)

print(task1)
arcpy.CreateHexagonsBySideLength_CreateHexagonsBySideLength(Study_Area=buffered_study_region, 
                                                            Hexagon_Side_Length=hex_side, 
                                                            Output_Hexagons = hex_grid)

task2 = 'make {} km buffer of feature {} to output feature {}'.format(float(hex_buffer)*0.001,hex_grid,hex_grid_buffer)
print(task2)
arcpy.Buffer_analysis(hex_grid, hex_grid_buffer, hex_buffer)

task = task1 + " and " + task2

# copy hex grid to postgis
command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" ' \
        + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
        + 'user={user} password = {pwd}" '.format(user = db_user,pwd = db_pwd) \
        + '{gdb} "{feature}" '.format(gdb = gdb_path,feature = hex_grid) \
        + '-lco geometry_name="geom"'
sp.call(command, shell=True)

# copy hex buffer to postgis
command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" ' \
        + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
        + 'user={user} password = {pwd}" '.format(user = db_user,pwd = db_pwd) \
        + '{gdb} "{feature}" '.format(gdb = gdb_path,feature = hex_grid_buffer) \
        + '-lco geometry_name="geom"'
sp.call(command, shell=True)


# # output to completion log					
script_running_log(script, task, start, locale)
