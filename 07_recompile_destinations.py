# Script:  recompile_destinations_gdb.py
# Purpose: This script recompiles the destinations geodatabase:
#             - converts multi-point to point where req'd
#             - clips to study region
#             - restricts to relevant destinations
#             - removes redundant columns
#             - compile as a single feature.
#             - A point ID is comma-delimited in form "Destionation,OID"
#               - this is to facilitate output to csv file following OD matrix calculation
#
# Author:  Carl Higgs
# Date:    05/07/2018

import arcpy
import time
import numpy
import json
import psycopg2
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

reload(sys)
sys.setdefaultencoding('utf8')

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])

# define spatial reference
spatial_reference = arcpy.SpatialReference(SpatialRef)

# ArcGIS environment settings
arcpy.env.workspace = src_destinations  
# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(os.path.join(temp,db)):
    os.makedirs(os.path.join(temp,db))
    
arcpy.env.scratchWorkspace = os.path.join(temp,db)  
scratchOutput = os.path.join(arcpy.env.scratchGDB,'MultiPointToPointDest')
arcpy.env.overwriteOutput = True 

# SQL set up for destination type table creation
queryPartA = "INSERT INTO dest_type VALUES "
sqlChunkify = 50
createTable = '''
  CREATE TABLE dest_type
  (dest integer NOT NULL,
   dest_name varchar PRIMARY KEY,
   dest_domain varchar NOT NULL,
   dest_count integer,
   dest_cutoff integer,
   dest_count_cutoff integer);
   '''

# OUTPUT PROCESS
# Compile restricted gdb of destination features
task = 'Recompile destinations from {} to study region gdb as combined feature {}'.format(dest_gdb,os.path.join(gdb,outCombinedFeature))
print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))

# Compile a list of datasets to be checked over for valid features within the destination GDB
datasets = arcpy.ListDatasets(feature_type='feature')

# Initialise empty destination count array (we fill this in below)
dest_count = numpy.empty(len(destination_list), dtype=int)
# we'll add a name field to the combined field list we create containing class of destination
max_name_length = len(max(destination_list, key=len))+3

# create new feature for combined destinations using a template
# Be aware that if the feature does exist, it will be overwritten
# If you are just wanting to add more destinations after having previously processed 
# you should comment the following two commands out, or place the following them
# in a condition such as the following:
# if arcpy.Exists(os.path.join(gdb_path,outCombinedFeature)) is False:  
arcpy.CreateFeatureclass_management(out_path = gdb_path, 
                                    out_name = outCombinedFeature,
                                    template = combined_dest_template)      
# Define projection to study region spatial reference
arcpy.DefineProjection_management(os.path.join(gdb_path,outCombinedFeature), spatial_reference)

                                      
for ds in datasets:
  for fc in arcpy.ListFeatureClasses(feature_dataset=ds):
    if fc in destination_list:
      destNum = destination_list.index(fc)
      # Make sure all destinations conform to shape type 'Point' (ie. not multipoint)
      if arcpy.Describe(fc).shapeType != u'Point':
        arcpy.FeatureToPoint_management(fc, scratchOutput, "INSIDE")
        arcpy.MakeFeatureLayer_management(scratchOutput,'destination')  
      else:
        # Select and copy destinations intersecting Melbourne hexes
        arcpy.MakeFeatureLayer_management(fc,'destination')                                            
      # clip to hex grid buffer
      selection = arcpy.SelectLayerByLocation_management('destination', 'intersect',os.path.join(gdb_path,hex_grid_buffer))
      count = int(arcpy.GetCount_management(selection).getOutput(0))
      dest_count[destNum] = count
      # Insert new rows in combined destination feature
      with arcpy.da.SearchCursor(selection,['SHAPE@','OID@']) as sCur:
        with arcpy.da.InsertCursor( os.path.join(gdb_path,outCombinedFeature),['SHAPE@','OBJECTID','dest_oid','dest_name']) as iCur:
          for row in sCur:
            dest_oid  = '{:02},{}'.format(dest_codes[destNum],row[1])
            dest_name = fc.encode('utf8')
            iCur.insertRow(row+(dest_oid, dest_name))

      # arcpy.Append_management('featureTrimmed', os.path.join(gdb_path,outCombinedFeature))
      print("Appended {} ({} points)".format(fc,count))

# Create destination type table in sql database
# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()
  
# drop table if it already exists
curs.execute("DROP TABLE IF EXISTS dest_type;")
conn.commit()
curs.execute(createTable)
conn.commit()

# insert values into table
# note that dest_count is feature count from above, not the dest_counts var from config
for i in range(0,len(destination_list)):
  curs.execute(queryPartA + "({},'{}','{}',{},{},{})".format(dest_codes[i],
                                                     destination_list[i],
                                                     dest_domains[i],
                                                     dest_count[i],  
                                                     dest_cutoffs[i],
                                                     dest_counts[i]) +' ON CONFLICT DO NOTHING')
  conn.commit()

print("Created 'dest_type' destination summary table for database {}.".format(db))
conn.close()
  
# output to completion log    
script_running_log(script, task, start, locale)