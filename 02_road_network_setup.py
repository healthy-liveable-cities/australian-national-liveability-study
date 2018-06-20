# Purpose: Set up road network for study region
# Author:  Carl Higgs
# Date:    20180612
#
# Note: This assumes you are runnign ArcGIS 10.6 (allows programatically building the network, etc)
#  Also assumes you have correct set up projections already - or otherwise defined them in the
#  config file under transform_method ---- this is used below but it assumes you have used the correct 'from' and 'to'
#  In this case, it is projecting from GCS GDA 1994 to GDA2020 GA LCC using the NTv2 grid

import arcpy
import time
import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'incorporate study region road network'

# ArcGIS environment settings
arcpy.env.workspace = folderPath  
# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(os.path.join(temp,db)):
    os.makedirs(os.path.join(temp,db))
    
arcpy.env.scratchWorkspace = os.path.join(temp,db)  
arcpy.env.overwriteOutput = True 
scratch_shape  = os.path.join(arcpy.env.scratchGDB,'scratch_shape')

SpatialRef = arcpy.SpatialReference(SpatialRef)

def basename(filePath):
  '''strip a path to the basename of file, without the extension.  Requires OS '''
  try: 
    return os.path.basename(os.path.normpath(filePath)).split(".",1)[0]
  except:
    print('Return basename failed. Did you import os?')

## NOTE This projection method may be scenario specific
##  I sought to copy a network data set from one location to another
##  Source data was GCS GDA 1994, and I wanted this to be GDA2020 GA LCC
##  Projecting the network dataset from one location to another does two jobs for price of one!
##  But - it is not in the spirit of generalisability, e.g.
####   - we otherwise intended to pre-process data in correct transformation
####   - the definition for transformation is baked in here (could put in config)    
# get transformation parameters

    
def clipFeature(feature,clippingFeature,where_clause,output):
  cliptask = 'Clipping feature ({}) to shape ({})...'.format(feature,clippingFeature)
  print(cliptask),
  try:
    arcpy.MakeFeatureLayer_management(feature, 'feature') 
    # arcpy.Project_management('feature', scratch_shape, spatial_reference)
    arcpy.Project_management(in_dataset = 'feature',
                         out_dataset = scratch_shape, 
                         out_coor_system = out_coor_system, 
                         transform_method = transform_method, 
                         in_coor_system = in_coor_system, 
                         preserve_shape="NO_PRESERVE_SHAPE", 
                         max_deviation="", 
                         vertical="NO_VERTICAL")
    arcpy.Delete_management('feature')
    arcpy.MakeFeatureLayer_management(scratch_shape, 'feature')
    arcpy.SelectLayerByLocation_management('feature', 'intersect',clippingFeature)
    if where_clause != ' ':
      # SQL Query where applicable
      arcpy.SelectLayerByAttribute_management('feature','SUBSET_SELECTION',where_clause)  
    arcpy.CopyFeatures_management('feature', output)
    print("Done.")
  except:
    print("ERROR: "+str(sys.exc_info()[0]))

arcpy.CheckOutExtension('Network')
# project network data into correct coordinate system in project gdb
arcpy.Project_management(in_dataset = network,
                         out_dataset=os.path.join(gdb_path,'{}'.format(network_source_feature_dataset)), 
                         out_coor_system = out_coor_system, 
                         transform_method = transform_method, 
                         in_coor_system = in_coor_system, 
                         preserve_shape="NO_PRESERVE_SHAPE", 
                         max_deviation="", 
                         vertical="NO_VERTICAL")
                         
# re-build the road network                         
arcpy.BuildNetwork_na(in_network_dataset=in_network_dataset_path)
arcpy.CheckInExtension('Network')  

clipFeature(intersections,buffered_study_region,' ','intersections')
 
#  Copy the intersections from gdb to postgis, correcting the projection in process
command = ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' \
       + ' PG:"host={host} port=5432 dbname={db}'.format(host = db_host,db = db) \
       + ' user={user} password = {pwd}" '.format(user = db_user,pwd = db_pwd) \
       + ' {gdb} "{feature}" '.format(gdb = gdb_path,feature = basename(intersections)) \
       + ' -lco geometry_name="geom"'
sp.call(command, shell=True)


# # output to completion log					
script_running_log(script, task, start, locale)