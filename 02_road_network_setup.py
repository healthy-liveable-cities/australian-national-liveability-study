# Purpose: Set up road network for study region
# Author:  Carl Higgs
# Date:    20180628
#
# This script constructs a road network from edges and nodes
#
# Specifically it is designed to be run on edges and nodes generated
# from Openstreetmap data using OSMnx.  
#
# In addition, it 
# - assumes a template network dataset has been constructed.
# - assumes use of ArcGIS 10.6 or later
# - projects the source nodes and edges from WGS84 to GDA2020 GA LCC
#   (or other projection type, as req'd depending on configuration file set up)

import arcpy
import time
# import psycopg2
# import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'incorporate study region road network'

# ArcGIS environment settings
arcpy.env.workspace = gdb_path  
arcpy.env.overwriteOutput = True 
SpatialReference = arcpy.SpatialReference(SpatialRef)

## NOTE This projection method will be scenario specific
# Using OSMnx data we project from 
# WGS84 to GDA2020 GA LCC using NTv2 transformation grid

print("Creating feature dataset to hold network..."),
if not arcpy.Exists(network_source_feature_dataset):
    arcpy.CreateFeatureDataset_management(gdb_path,
                                          network_source_feature_dataset, 
                                          spatial_reference = SpatialReference)
print(" Done.")

for feature in ['edges','nodes']:
  if not arcpy.Exists(os.path.join('{}'.format(network_source_feature_dataset),feature)):
      print("Project {} to feature dataset in {}...".format(feature,SpatialRef)),
      arcpy.Project_management(in_dataset = os.path.join(network_source,
                                                         feature,
                                                         '{}.shp'.format(feature)),
                             out_dataset=os.path.join('{}'.format(network_source_feature_dataset), 
                                                      feature),
                             out_coor_system = out_coor_system, 
                             transform_method = network_transform_method, 
                             in_coor_system = network_in_coor_system, 
                             preserve_shape="NO_PRESERVE_SHAPE", 
                             max_deviation="", 
                             vertical="NO_VERTICAL")
      print(" Done.")

arcpy.CheckOutExtension('Network')
# # The below process assumes a network dataset template has been created
# # This was achieved for the current OSMnx schema with the below code
# arcpy.CreateTemplateFromNetworkDataset_na(network_dataset="D:/ntnl_li_2018_template/data/li_melb_2016_osmnx.gdb/PedestrianRoads/PedestrianRoads_ND", 
                                          # output_network_dataset_template="D:/ntnl_li_2018_template/data/roads/osmnx_nd_template.xml")
if not arcpy.Exists(network_source_feature_dataset):
    print("Creating network dataset from template..."),                                          
    arcpy.CreateNetworkDatasetFromTemplate_na(network_template, 
                                              network_source_feature_dataset)
    print(" Done.")
                        
# build the road network       
print("Build network..."),                  
arcpy.BuildNetwork_na(in_network_dataset)
print(" Done.")
arcpy.CheckInExtension('Network')  

# # output to completion log					
script_running_log(script, task, start, locale)