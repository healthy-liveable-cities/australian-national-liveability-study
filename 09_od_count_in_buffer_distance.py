# Script:  createODmatrix_Loop_parallelised_closestAB.py
# Purpose: This script finds for each A point the closest B point along a network.
#              - it uses parallel processing
#              - it outputs to an sql database 
# Authors: Carl Higgs, Koen Simons
#
# Note: Following processing, I would recommend you check out the log_od_distances table 
# in postgresql and consider the entries with 'no solution' - are these reasonable?
# For example - in psql run query 
# SELECT * FROM log_od_counts WHERE status = 'no solution' ORDER BY random() limit 20;
# Now, using ArcMap check out those hexes and destinations - can you explain why there 
# was no solution?  In my trial I was using a demo road network feature, and such results
# returned where parcels could not be snapped to a road network.  So, these results should 
# be considered critically, if they occur.  Is it a failing in our process, and if so can
# we fix it?

import arcpy, arcinfo
import os
import time
import multiprocessing
import sys
import psycopg2 
import numpy as np
from progressor import progressor

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])

# define spatial reference
spatial_reference = arcpy.SpatialReference(SpatialRef)

# ArcGIS environment settings
arcpy.env.workspace = gdb_path  
# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(os.path.join(temp,db)):
    os.makedirs(os.path.join(temp,db))
    
arcpy.env.scratchWorkspace = os.path.join(temp,db)  
arcpy.env.qualifiedFieldNames = False  
arcpy.env.overwriteOutput = True 

# Specify geodatabase with feature classes of "origins"
origin_points   = parcel_dwellings
origin_pointsID = points_id

## specify "destination_points" (e.g. destinations)
destination_pointsID = destination_id

# Get a list of feature 
featureClasses = arcpy.ListFeatureClasses()

# Processing is undertake for any value > hexStart
# So, if you want to start from a specific hex number,
# you could change this to a larger value
hexStart = 0

# SQL Settings
## Note - this used to be 'dist_cl_od_parcel_dest' --- simplified to 'od_distances'
od_distances = "od_counts"
log_table = "log_od_counts"
queryPartA = "INSERT INTO {} VALUES ".format(od_distances)

sqlChunkify = 500
  
# initiate postgresql connection
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

# get list of hexes over which to iterate
curs.execute("SELECT hex FROM hex_parcels;")
hex_list = list(curs)    

# define reduced set of destinations and cutoffs (ie. only those with cutoffs defined)
destination_list = np.array(destination_list)[np.array([x!='NULL' for x in dest_counts])]
dest_counts = np.array(dest_counts)[np.array([x!='NULL' for x in dest_counts])]
dest_codes = np.array(dest_codes)[np.array([x!='NULL' for x in dest_counts])]

# tally expected hex-destination result set  
completion_goal = len(hex_list)*len(destination_list)

# get pid name
pid = multiprocessing.current_process().name

# Define query to create table
createTable     = '''
  DROP TABLE IF EXISTS {0};
  CREATE TABLE IF NOT EXISTS {0}
  ({1} varchar NOT NULL ,
   dest smallint NOT NULL ,
   dest_name varchar NOT NULL ,
   cutoff integer NOT NULL, 
   count integer NOT NULL, 
   PRIMARY KEY({1},dest)
   );
   '''.format(od_distances, origin_pointsID)

queryPartA      = '''
  INSERT INTO {} VALUES
  '''.format(od_distances)

createTable_log     = '''
    DROP TABLE IF EXISTS {0};
    CREATE TABLE IF NOT EXISTS {0}
      (hex integer NOT NULL, 
      parcel_count integer NOT NULL, 
      dest varchar, 
      status varchar, 
      mins double precision,
      PRIMARY KEY(hex,dest)
      );
          '''.format(log_table)    

queryInsert      = '''
  INSERT INTO {} VALUES
  '''.format(log_table)          

queryUpdate      = '''
  ON CONFLICT ({0},{4}) 
  DO UPDATE SET {1}=EXCLUDED.{1},{2}=EXCLUDED.{2},{3}=EXCLUDED.{3}
  '''.format('hex','parcel_count','status','mins','dest')            
    
# Define log file write method
def writeLog(hex = 0, AhexN = 'NULL', Bcode = 'NULL', status = 'NULL', mins= 0, create = log_table):
  try:
    if create == 'create':
      curs.execute(createTable_log)
      conn.commit()
      
    else:
      moment = time.strftime("%Y%m%d-%H%M%S")
      # print to screen regardless
      # print('Hex:{:5d} A:{:8s} Dest:{:8s} {:15s} {:15s}'.format(hex, str(AhexN), str(Bcode), status, moment))     
      # write to sql table
      curs.execute("{0} ({1},{2},'{3}','{4}',{5}) {6}".format(queryInsert,hex, AhexN, Bcode,status, mins, queryUpdate))
      conn.commit()  
  except:
    print("ERROR: {}".format(sys.exc_info()))
    raise

# Worker/Child PROCESS
def ODMatrixWorkerFunction(hex): 
  # Connect to SQL database 
  try:
    conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
    curs = conn.cursor()
  except:
    print("SQL connection error")
    print(sys.exc_info()[1])
    return 100
  # make sure Network Analyst licence is 'checked out'
  arcpy.CheckOutExtension('Network')
 
  # Worker Task is hex-specific by definition/parallel
  # Skip if hex was finished in previous run
  hexStartTime = time.time()
  if hex < hexStart:
    return(1)
    
  try:    
    # select origin points 
    arcpy.MakeFeatureLayer_management (origin_points, "origin_points_layer")
    origin_selection = arcpy.SelectLayerByAttribute_management("origin_points_layer", where_clause = 'HEX_ID = {}'.format(hex))
    origin_point_count = int(arcpy.GetCount_management(origin_selection).getOutput(0))
    # Skip hexes with zero adresses
    if origin_point_count == 0:
        writeLog(hex,0,'NULL',"no origin points",(time.time()-hexStartTime)/60)
        return(2)
    
    # make destination feature layer
    arcpy.MakeFeatureLayer_management (outCombinedFeature, "destination_points_layer")        
        
    # store list of destinations with counts (so as to overlook destinations for which zero data exists!)
    curs.execute("SELECT dest_name,dest_count FROM dest_type")
    count_list = list(curs)         
    
    # fetch list of successfully processed destinations for this hex, if any
    curs.execute("SELECT dest FROM {} WHERE hex = {}".format(log_table,hex))
    completed_dest = [x[0] for x in list(curs)]
    remaining_dest_list = [x for x in destination_list if x not in completed_dest]
    
    for destination_points in remaining_dest_list:
      destStartTime = time.time()
      destNum = np.where(destination_list==destination_points)[0][0]
      # only procede if > 0 destinations of this type are present in study region
      if count_list[destNum][1] == 0:
        writeLog(hex,origin_point_count,destination_list[destNum],"no dest in study region",(time.time()-destStartTime)/60)
        
      if count_list[destNum][1] > 0:    
        if dest_counts[destNum]!='NULL':
          dest_count_threshold = int(float(dest_counts[destNum]))        
          # select destination points 
          destination_selection = arcpy.SelectLayerByAttribute_management("destination_points_layer", where_clause = "dest_name = '{}'".format(destination_points))
          # OD Matrix Setup
          
          # Make OD cost matrix layer
          result_object = arcpy.MakeODCostMatrixLayer_na(in_network_dataset = in_network_dataset, 
                                                         out_network_analysis_layer = "ODmatrix", 
                                                         impedance_attribute = "Length",  
                                                         default_cutoff = dest_count_threshold,
                                                         UTurn_policy = "ALLOW_UTURNS", 
                                                         hierarchy = "NO_HIERARCHY", 
                                                         output_path_shape = "NO_LINES")
          outNALayer = result_object.getOutput(0)
          
          #Get the names of all the sublayers within the service area layer.
          subLayerNames = arcpy.na.GetNAClassNames(outNALayer)
          #Store the layer names that we will use later
          originsLayerName = subLayerNames["Origins"]
          destinationsLayerName = subLayerNames["Destinations"]
          linesLayerName = subLayerNames["ODLines"]
          
          # you may have to do this later in the script - but try now....
          ODLinesSubLayer = arcpy.mapping.ListLayers(outNALayer, linesLayerName)[0]
          fields = ['Name', 'Total_Length']
          
          arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
              sub_layer                      = originsLayerName, 
              in_table                       = origin_selection, 
              field_mappings                 = "Name {} #".format(origin_pointsID), 
              search_tolerance               = "{} Meters".format(tolerance), 
              search_criteria                = "{} SHAPE;{} NONE".format(network_edges,network_junctions), 
              append                         = "CLEAR", 
              snap_to_position_along_network = "NO_SNAP", 
              exclude_restricted_elements    = "INCLUDE",
              search_query                   = "{} #;{} #".format(network_edges,network_junctions))
          
          arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
              sub_layer                      = destinationsLayerName, 
              in_table                       = destination_selection, 
              field_mappings                 = "Name {} #".format(destination_pointsID), 
              search_tolerance               = "{} Meters".format(tolerance), 
              search_criteria                = "{} SHAPE;{} NONE".format(network_edges,network_junctions), 
              append                         = "CLEAR", 
              snap_to_position_along_network = "NO_SNAP", 
              exclude_restricted_elements    = "INCLUDE",
              search_query                   = "{} #;{} #".format(network_edges,network_junctions))
          
          # Process: Solve
          result = arcpy.Solve_na(outNALayer, terminate_on_solve_error = "CONTINUE")
          if result[1] == u'false':
            writeLog(hex,origin_point_count,destination_list[destNum],"no solution",(time.time()-destStartTime)/60)
          else:
            # get dest_code for feature from dest_type table
            curs.execute("SELECT dest FROM dest_type WHERE dest_name = '{}'".format(destination_points))
            dest_code = list(curs)[0][0]
            # Extract lines layer, export to SQL database
            df = arcpy.da.TableToNumPyArray(ODLinesSubLayer, 'Name')    
            stripped_df = [f[0].encode('utf-8').split(' - ')[0] for f in df]
            id_counts = np.unique(stripped_df, return_counts=True)
            length  = len(id_counts[0])-1
            count = 0
            chunkedLines = list()
            place = "before loop"
            for x in range(0,length) :
              count += 1
              ID = id_counts[0][x]
              tally = id_counts[1][x]
              chunkedLines.append("('{}',{},'{}',{},{})".format(ID,dest_code,destination_points,dest_count_threshold,tally))
              if(count % sqlChunkify == 0):
                place = "before postgresql out"
                curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines))
                conn.commit()
                chunkedLines = list() 
            if(count % sqlChunkify != 0):
              curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines))
              conn.commit()
            writeLog(hex,origin_point_count,destination_list[destNum],"Solved",(time.time()-destStartTime)/60)
    # return worker function as completed once all destinations processed
    return 0
  
  except:
    print(sys.exc_info())
    
  finally:
    arcpy.CheckInExtension('Network')
    # Report on progress
    curs.execute("SELECT count(*) FROM {}".format(log_table))
    progress = int(list(curs)[0][0]) 
    progressor(progress,completion_goal,start,"{numerator} / {denominator} hex-destination combinations processed.".format(numerator = progress,denominator = completion_goal))
    # Close SQL connection
    conn.close()

    
# MAIN PROCESS
if __name__ == '__main__':
  # Task name is now defined
  task = 'Count destinations within network buffer distance of origins'  # Do stuff
  print("Commencing task ({}): {} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))
  
  try:
    conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
    curs = conn.cursor()

    # create OD matrix table
    curs.execute(createTable)
    conn.commit()

  except:
    print("SQL connection error")
    print(sys.exc_info()[0])
    raise
   
  print("Initialise log file..."),
  writeLog(create='create')
  print(" Done.")
  
  print("Setup a pool of workers/child processes and split log output..."),
  # Parallel processing setting
  nWorkers = 4    
  pool = multiprocessing.Pool(processes=nWorkers)
  print(" Done.")

  print("Iterate over hexes...")
  iteration_list = np.asarray([x[0] for x in hex_list])
  # # Iterate process over hexes across nWorkers
  pool.map(ODMatrixWorkerFunction, iteration_list, chunksize=1)
  
  # output to completion log    
  script_running_log(script, task, start, locale)
  conn.close()
