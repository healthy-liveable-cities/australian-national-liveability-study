# Script:  08_od_distances_psma_osm_comparison.py
# Purpose: This script finds for each A point the closest B point along a network.
#              - it uses parallel processing
#              - it outputs to an sql database 
#              - IT IS HARD CODED TO USE 2013 CLEAN ROADS PSMA NETWORK
#              - it is intended to be used to compare od results using osm (other script) and psma
# Authors: Carl Higgs

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
od_distances  = "od_psma"
log_table    = "log_od_psma"
queryPartA = "INSERT INTO {} VALUES ".format(od_distances)

sqlChunkify = 500
  
# initiate postgresql connection
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

# get pid name
pid = multiprocessing.current_process().name

# Define query to create table
createTable     = '''
  DROP TABLE IF EXISTS {0};
  CREATE TABLE IF NOT EXISTS {0}
  ({1} varchar NOT NULL ,
   dest smallint NOT NULL ,
   oid bigint NOT NULL ,
   distance integer NOT NULL, 
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
      curs.execute("{0} ({1},{2},{3},'{4}',{5}) {6}".format(queryInsert,hex, AhexN, Bcode,status, mins, queryUpdate))
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
    origin_selection = arcpy.SelectLayerByAttribute_management("origin_points_layer", where_clause = 'hex_id = {}'.format(hex))
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
    completed_dest_in_hex = list(curs)
    
    # completed destination IDs need to be selected as first element in tuple, and converted to integer
    completed_dest = [destination_list[int(x[0])] for x in completed_dest_in_hex if destination_list[int(x[0])] not in completed_dest_in_hex]
    remaining_dest_list = [x for x in destination_list if x not in completed_dest]
    
    # Make OD cost matrix layer
    result_object = arcpy.MakeODCostMatrixLayer_na(in_network_dataset = psma_network_dataset, 
                                                   out_network_analysis_layer = "ODmatrix", 
                                                   impedance_attribute = "Length", 
                                                   default_number_destinations_to_find = 1,
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
    
    for destination_points in remaining_dest_list:
      destStartTime = time.time()
      destNum = destination_list.index(destination_points)
      # only procede if > 0 destinations of this type are present in study region
      if count_list[destNum][1] == 0:
        writeLog(hex,origin_point_count,destNum,"no dest in study region",(time.time()-destStartTime)/60)
      if count_list[destNum][1] > 0:
        # select destination points 
        destination_selection = arcpy.SelectLayerByAttribute_management("destination_points_layer", where_clause = "dest_name = '{}'".format(destination_points))
        # OD Matrix Setup
        arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
            sub_layer                      = originsLayerName, 
            in_table                       = origin_selection, 
            field_mappings                 = "Name {} #".format(origin_pointsID), 
            search_tolerance               = "{} Meters".format(tolerance), 
            search_criteria                = "{} SHAPE;{} NONE".format(psma_network_edges,psma_network_junctions), 
            append                         = "CLEAR", 
            snap_to_position_along_network = "NO_SNAP", 
            exclude_restricted_elements    = "INCLUDE",
            search_query                   = "{} #;{} #".format(psma_network_edges,psma_network_junctions))
        arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
            sub_layer                      = destinationsLayerName, 
            in_table                       = destination_selection, 
            field_mappings                 = "Name {} #".format(destination_pointsID), 
            search_tolerance               = "{} Meters".format(tolerance), 
            search_criteria                = "{} SHAPE;{} NONE".format(psma_network_edges,psma_network_junctions), 
            append                         = "CLEAR", 
            snap_to_position_along_network = "NO_SNAP", 
            exclude_restricted_elements    = "INCLUDE",
            search_query                   = "{} #;{} #".format(psma_network_edges,psma_network_junctions))
        # Process: Solve
        result = arcpy.Solve_na(outNALayer, terminate_on_solve_error = "CONTINUE")
        if result[1] == u'false':
          writeLog(hex,origin_point_count,destNum,"no solution",(time.time()-destStartTime)/60)
        else:
          # Extract lines layer, export to SQL database
          outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)
          count = 0
          chunkedLines = list()
          for outputLine in outputLines :
            count += 1
            ID = outputLine[0].split('-')
            ID1 = ID[1].split(',')
            chunkedLines.append("('{}',{},{},{})".format(ID[0].strip(' '),ID1[0],ID1[1],int(round(outputLine[1]))))
            if(count % sqlChunkify == 0):
              curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines))
              conn.commit()
              chunkedLines = list()
          if(count % sqlChunkify != 0):
            curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines))
            conn.commit()
          writeLog(hex,origin_point_count,destNum,"Solved",(time.time()-destStartTime)/60)
    # return worker function as completed once all destinations processed
    return 0
  except:
    writeLog(hex, multiprocessing.current_process().pid, "Error: ",sys.exc_info()[1], (time.time()-hexStartTime)/60)
    return(multiprocessing.current_process().pid)
  finally:
    arcpy.CheckInExtension('Network')
    # Report on progress
    curs.execute("SELECT count(*) FROM {}".format(log_table))
    progress = int(list(curs)[0][0]) 
    progressor(progress,completion_goal,start,"{numerator} / {denominator} hex-destination combinations processed.".format(numerator = progress,denominator = completion_goal))
    # Close SQL connection
    conn.close()

# get list of hexes over which to iterate
curs.execute("SELECT hex FROM hex_parcels;")
hex_list = list(curs)    

# tally expected hex-destination result set  
completion_goal = len(hex_list)*len(destination_list)
    
# MAIN PROCESS
if __name__ == '__main__':
  # Task name is now defined
  task = 'Find closest of each destination type to origin'  # Do stuff
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

  # Create sausage buffer spatial index
  print("Creating sausage buffer spatial index... "),
  curs.execute("CREATE INDEX IF NOT EXISTS {0}_gix ON {0} USING GIST (geom);".format(od_distances))
  conn.commit()
  print("Done.")  
  
  # output to completion log    
  conn.close()
  script_running_log(script, task, start, locale)
