# Script:  area_linkage_tables.py
# Purpose: Create ABS and non-ABS linkage tables using 2016 data sourced from ABS
#
#          Parcel address points are already associated with Meshblock in the sample_point_feature table
#          Further linkage with the abs_linkage table (actually, a reduced version of the existing mb_dwellings)
#          facilitates aggregation to ABS area units such as SA1, SA2, SA3, SA4.
#
#          The non-ABS linkage table associated points with the suburb and LGA in which they are located, according
#          to ABS sourced spatial features.
#
#          Regarding linkage of addresses with non-ABS structures, while the ABS provides some 
#          correspondence tables between areas, (e.g. SA2 2016 to LGA 2016) this will not be as accurate
#          for our purposes as taking an address point location and evaluating the polygon it intersects.
#          There are pitfalls in this approach (e.g. if a point lies exactly on a boundary), however
#          this is par for the course when generalising unique units into aggregate categories 
#          (ie. points to averages, sums or variances within contiguous areas).
# 
# Author:  Carl Higgs
# Date:    20180710

# Import arcpy module

import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import numpy
import time
import psycopg2 
from progressor import progressor
from sqlalchemy import create_engine
# importing arcpy as legacy dependency (using resource in gdb for network analysis)
import arcpy

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *


# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = '\nCreate area linkage tables using 2016 data sourced from ABS'

print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))
# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()


## ArcGIS code (currently necessary due to legacy usage in lead up to network analysis)
# ArcGIS environment settings
arcpy.env.workspace = locale_dir  
# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(os.path.join(temp,db)):
    os.makedirs(os.path.join(temp,db))
    
# define spatial reference
SpatialRef = arcpy.SpatialReference(SpatialRef)

# OUTPUT PROCESS

features = ['area_linkage',
            '{}'.format(study_region),
            '{}'.format(buffered_study_region),
            sample_point_feature,
            'footprints',
            'edges',
            'nodes',
            'mb_dwellings']
processing_gpkg = os.path.join(folderPath,'study_region',locale,'{}_processing.gpkg'.format(locale))
command = (
        ' ogr2ogr -overwrite -f GPKG  '
        ' {gpkg} ' 
        ' PG:"host={host} port=5432 dbname={db} user={user} password = {pwd}" '
        ' {tables} '.format(gpkg = processing_gpkg,
                              host = db_host,
                              db = db,
                              user = db_user,
                              pwd = db_pwd,
                              tables = ' '.join(features)
                              )
)                              
sp.call(command, shell=True)

# Create output gdb if not already existing
if os.path.exists(gdb_path):
  print("Using extant file geodatabase: {}".format(gdb_path)) 
else:
  arcpy.CreateFileGDB_management(locale_dir,gdb)
  print("File geodatabase created: {}".format(gdb_path))

# copy study region, buffered study region and mb_dwellings to arcgis
arcpy.env.workspace = db_sde_path
arcpy.env.overwriteOutput = True


fcList = arcpy.ListFeatureClasses()
for fc in fcList:
    name = fc.split('.')[2]
    if name in features:
        feature = fc
        print(feature)
        try:
            if arcpy.Exists('{}'.format(feature)):
                arcpy.CopyFeatures_management(os.path.join(processing_gpkg,feature), os.path.join(gdb_path,name))
            else:
                print("It seems that the feature doesn't exist...")
        except:
           print("... that didn't work ...")
       
# output to completion log					
script_running_log(script, task, start, locale)
conn.close()
