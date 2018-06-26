# Purpose: Prepare POS for ntnl liveability indicators
#           -- *** Assumes already in correct projection for project (e.g. GDA2020 GA LCC) *** 
#           -- copies features within study region to project gdb
#           -- calculates geodesic area in hectares
#           -- makes temporary line feature from polygons
#           -- traces vertices at set interval (pos_vertices in config file)
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
task = 'Clip address to study region, dissolve by location counting collapse degree'

# ArcGIS environment settings
arcpy.env.workspace = gdb_path  
arcpy.ImportToolbox(os.path.join(folderPath,CreatePointsLines_tbx))

# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(os.path.join(temp,db)):
    os.makedirs(os.path.join(temp,db))
    
arcpy.env.scratchWorkspace = os.path.join(temp,db)  
arcpy.env.overwriteOutput = True 

print("Copying POS features within study region to database..."),
arcpy.MakeFeatureLayer_management(pos_source, 'feature') 
arcpy.SelectLayerByLocation_management('feature', 'intersect',"gccsa_2016")
arcpy.CopyFeatures_management('feature', "pos_shape")
print(" Done.")

print("Calculating geodesic area in hectares..."),
arcpy.AddField_management("pos_shape", "area_ha", "DOUBLE")

arcpy.CalculateField_management("pos_shape", "area_ha", "!shape.geodesicArea@hectares!", "PYTHON")
print(" Done.")

print("Creating points at {}m intervals...".format(pos_vertices)),
arcpy.PolygonToLine_management("pos_shape", os.path.join(arcpy.env.scratchGDB,"pos_line"), "IGNORE_NEIGHBORS")

arcpy.CreatePointsLines_CreatePointsLines(Input_Polyline_Feature_Class="pos_line", 
                                          Type="INTERVAL BY DISTANCE", 
                                          Starting_Location="BEGINNING", 
                                          Use_Field_to_Set_Value_="NO", 
                                          Field_with_Value="", 
                                          Distance___Percentage_Value="{}".format(pos_vertices), 
                                          Add_End_Points_="BOTH", 
                                          Output_Point_Feature_Class= "pos_50m_vertices")
print(" Done.") 

# gdb to pgsql
print("Copy the pos points with area attribute data to PostgreSQL database..."),
command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" ' \
        + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
        + 'user={user} password = {pwd} " '.format(user = db_user,pwd = db_pwd) \
        + '{gdb} "{feature}" '.format(gdb = gdb_path,feature = "pos_50m_vertices") \
        + '-lco geometry_name="geom" '
sp.call(command, shell=True)

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()
curs.execute(grant_query)
conn.commit()
conn.close()
print(" Done.") 
 
# output to completion log    
script_running_log(script, task, start, locale)
