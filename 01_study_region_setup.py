# Script:  01_study_region_setup.py
# Purpose: Python set up study region boundaries
# Author:  Carl Higgs
# Date:    2018 06 05

import arcpy
import time
import psycopg2
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create study region boundary files in new geodatabase'


# ArcGIS environment settings
arcpy.env.workspace = locale_dir  
# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(os.path.join(temp,db)):
    os.makedirs(os.path.join(temp,db))
    
arcpy.env.scratchWorkspace = os.path.join(temp,db)  
arcpy.env.overwriteOutput = True 

# define spatial reference
SpatialRef = arcpy.SpatialReference(SpatialRef)

# OUTPUT PROCESS
# Create output gdb if not already existing
if os.path.exists(gdb_path):
  print("Using extant file geodatabase: {}".format(gdb_path)) 
if not os.path.exists(gdb_path):
  arcpy.CreateFileGDB_management(locale_dir,gdb)
  print("File geodatabase created: {}".format(gdb_path))

print("Feature: {}".format(region_shape))
print("Query: {}".format(region_where_clause))
arcpy.MakeFeatureLayer_management(r"{}".format(region_shape),'feature') 

# select subset of features to be included
arcpy.SelectLayerByAttribute_management(in_layer_or_view  = 'feature', 
                                          selection_type    = "NEW_SELECTION", 
                                          where_clause      = "{}".format(region_where_clause))  
# create copy of selected features as new feature class
# in gdb
arcpy.CopyFeatures_management('feature',os.path.join(gdb,study_region))

## Buffer study region
arcpy.env.workspace = gdb
arcpy.env.overwriteOutput = True 
# make buffer
arcpy.Buffer_analysis(study_region, buffered_study_region, study_buffer)

print("copy study region to postgis...")
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

print("Copy buffered study region to postgis...")
command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" ' \
        + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
        + 'user={user} password = {pwd}" '.format(user = db_user,pwd = db_pwd) \
        + '{gdb} "{feature}" '.format(gdb = gdb_path,feature = buffered_study_region) \
        + '-lco geometry_name="geom"'
sp.call(command, shell=True)

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()
curs.execute(grant_query)
conn.commit()


## The below step projects buffered study region from GDA2020 GA LCC to a WGS84 shape file
# This buffered study region polygon is used to source OSMnx Pedestrian network
locale_4326_shp = os.path.join(locale_dir,'{}_{}_{}m_epsg4326.shp'.format(locale.lower(),study_region,study_buffer))
arcpy.Project_management(in_dataset=os.path.join(locale_dir,'{}.gdb/{}_{}m'.format(db,study_region,study_buffer)), 
                         out_dataset=locale_4326_shp, 
                         out_coor_system="GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]", 
                         transform_method="'GDA_1994_To_GDA2020_NTv2_CD + GDA_1994_To_WGS_1984'", 
                         in_coor_system="PROJCS['GDA2020_GA_LCC',GEOGCS['GDA2020',DATUM['GDA2020',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',134.0],PARAMETER['Standard_Parallel_1',-18.0],PARAMETER['Standard_Parallel_2',-36.0],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]]", 
                         preserve_shape="NO_PRESERVE_SHAPE",
                         max_deviation="", 
                         vertical="NO_VERTICAL")                          
                   

# output to completion log					
script_running_log(script, task, start, locale)
conn.close()
