# Script:  21_createodmatrix_loop_parallelised_pos_largepark.py
# Purpose: This script creates OD matrix 
#          It is intended to find distance from parcel to closest POS (of any size)
# Input:   requires network dataset
# Authors: Carl Higgs, Koen Simons

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
task = 'OD matrix - distance from parcel to closest POS of any size'

# INPUT PARAMETERS

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

## specify "destinations"
pos_points   =  'pos_50m_vertices'
pos_pointsID =  'pos_entryid'

hexStart = 0

# SQL Settings
sqlTableName  = "od_pos"
log_table    = "log_od_distances"
queryPartA = "INSERT INTO {} VALUES ".format(sqlTableName)

sqlChunkify = 500
        
# initiate postgresql connection
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

# get pid name
pid = multiprocessing.current_process().name
# create initial OD cost matrix layer on worker processors
if pid !='MainProcess':
  # Make OD cost matrix layer
  result_object = arcpy.MakeODCostMatrixLayer_na(in_network_dataset = in_network_dataset, 
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
  
  arcpy.MakeFeatureLayer_management(hex_grid_buffer, "buffer_layer")                
  
  
# Define query to create table
createTable     = '''
  -- DROP TABLE IF EXISTS {0};
  CREATE TABLE IF NOT EXISTS {0}
  (gnaf_pid    varchar         ,
   pos_id     varchar         ,
   query      varchar         ,
   distance   double precision,
   threshold  int             ,
   ind_hard   int             ,
   ind_soft   double precision
   );
   '''.format(sqlTableName, origin_pointsID.lower())
## LATER index on gnaf_id, query

queryPartA      = '''
  INSERT INTO {} VALUES
  '''.format(sqlTableName)

# this is the same log table as used for other destinations.
#  It is only created if it does not already exist.
#  However, it probably does.  
createTable_log     = '''
  -- DROP TABLE IF EXISTS {0};
  CREATE TABLE IF NOT EXISTS {0}
    (hex integer NOT NULL, 
    parcel_count integer NOT NULL, 
    dest_name varchar, 
    status varchar, 
    mins double precision,
    PRIMARY KEY(hex,dest_name)
    );
    '''.format(log_table)     
  
queryInsert      = '''
  INSERT INTO {} VALUES
  '''.format(log_table)          
                    
queryUpdate      = '''
  ON CONFLICT ({0},{4}) 
  DO UPDATE SET {1}=EXCLUDED.{1},{2}=EXCLUDED.{2},{3}=EXCLUDED.{3}
  '''.format('hex','parcel_count','status','mins','dest_name')  

parcel_count = int(arcpy.GetCount_management(origin_points).getOutput(0))  
denominator = parcel_count * len(pos_locale)
 
primary_key = '''
  ALTER TABLE {table} ADD COLUMN seq serial NOT NULL UNIQUE;
  DELETE FROM {table} del
    WHERE EXISTS(
        SELECT*FROM {table} x
        WHERE x.{point_id}=del.{point_id}
        AND   x.query=del.query
        AND   x.threshold=del.threshold
        AND x.seq < del.seq
        ); 
 ALTER  TABLE {table} ADD PRIMARY KEY ({point_id},query,threshold);
 '''.format(table = sqlTableName, point_id = points_id)
 
## Functions defined for this script
# Define log file write method
def writeLog(hex = 0, AhexN = 'NULL', Bcode = 'NULL', status = 'NULL', mins= 0, create = log_table):
  try:
    if create == 'create':
      curs.execute(createTable_log)
      conn.commit()
      
    else:
      moment = time.strftime("%Y%m%d-%H%M%S")
  
      # write to sql table
      curs.execute("{0} ({1},{2},'{3}','{4}',{5}) {6}".format(queryInsert,hex, AhexN, Bcode,status, mins, queryUpdate))
      conn.commit()  
  except:
    print("ERROR: {}".format(sys.exc_info()))
    raise


def unique_values(table, field):
  data = arcpy.da.TableToNumPyArray(table, [field])
  return np.unique(data[field])    
    
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
  #     Skip if hex was finished in previous run
  hexStartTime = time.time()
  if hex < hexStart:
    return(1)
    
  try:
    arcpy.MakeFeatureLayer_management (origin_points, "origin_pointsLayer")
    place = 'before A_selection'
    A_selection = arcpy.SelectLayerByAttribute_management("origin_pointsLayer", where_clause = 'hex_id = {}'.format(hex))   
    A_pointCount = int(arcpy.GetCount_management(A_selection).getOutput(0))
    place = 'before skip empty A hexes'
    # Skip empty A hexes
    if A_pointCount == 0:
      writeLog(hex,0,"pos","no A points",(time.time()-hexStartTime)/60)
      return(2)
    
    place = 'before buffer selection'
    buffer = arcpy.SelectLayerByAttribute_management("buffer_layer", where_clause = 'ORIG_FID = {}'.format(hex))
    
    # loop over POS scenarios for this study region
    for query in pos_locale:
      # ensure only non-processed parcels are processed (ie. in case script has been previously run)
      curs.execute("SELECT {} FROM {} WHERE query = '{}'".format(origin_pointsID,sqlTableName,hex,query[0]))
      processed_points = [x[0] for x in list(curs)]
      # Only procede with the POS scenario if it has not been previously processed
      if len(processed_points) < A_pointCount:
        # if len(processed_points)!=0:
          # A_selection = arcpy.SelectLayerByAttribute_management(A_selection, selection_type = 'REMOVE_FROM_SELECTION', where_clause = '{0} IN ({1})'.format(origin_pointsID,','.join(processed_points)))
        arcpy.MakeFeatureLayer_management(pos_points, "pos_pointsLayer", query[0])    
        
        # Select and count parks meeting scenario query
        B_selection = arcpy.SelectLayerByLocation_management('pos_pointsLayer', 'intersect', buffer)
        B_pointCount = int(arcpy.GetCount_management(B_selection).getOutput(0))
        
        # define query and distance as destination code, for use in log file 
        dest_code = "{} @ {}m".format(query[0],query[1])
        dest_name = "POS: {}".format(dest_code)
        
        place = 'before skip empty B hexes'
        # Skip empty B hexes
        if B_pointCount != 0:          
          # If we're still in the loop at this point, it means we have the right hex and buffer combo and both contain at least one valid element
          # Process OD Matrix Setup
          # add unprocessed address points
          arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
              sub_layer                      = originsLayerName, 
              in_table                       = A_selection, 
              field_mappings                 = "Name {} #".format(origin_pointsID), 
              search_tolerance               = "{} Meters".format(tolerance), 
              search_criteria                = "{} SHAPE;{} NONE".format(network_edges,network_junctions), 
              append                         = "CLEAR", 
              snap_to_position_along_network = "NO_SNAP", 
              exclude_restricted_elements    = "INCLUDE",
              search_query                   = "{} #;{} #".format(network_edges,network_junctions))
          place = 'After add A locations'
          # add in parks
          arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
            sub_layer                      = destinationsLayerName, 
            in_table                       = B_selection, 
            field_mappings                 = "Name {} #".format(pos_pointsID), 
            search_tolerance               = "{} Meters".format(tolerance), 
            search_criteria                = "{} SHAPE;{} NONE".format(network_edges,network_junctions), 
            append                         = "CLEAR", 
            snap_to_position_along_network = "NO_SNAP", 
            exclude_restricted_elements    = "INCLUDE",
            search_query                   = "{} #;{} #".format(network_edges,network_junctions))    
          place = 'After add B locations'
          # Process: Solve
          result = arcpy.Solve_na(outNALayer, terminate_on_solve_error = "CONTINUE")
          if result[1] == u'false':
            writeLog(hex,A_pointCount,dest_name,"no solution",(time.time()-hexStartTime)/60)
          if result[1] == u'true':
            place = 'After solve'
            # Extract lines layer, export to SQL database
            outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)
            curs = conn.cursor()
            count = 0
            chunkedLines = list()
            place = 'before outputLine loop'
            for outputLine in outputLines :
              count += 1
              ID_A      = outputLine[0].split('-')[0].encode('utf-8').strip(' ')
              ID_B      = outputLine[0].split('-')[1].encode('utf-8').strip(' ')
              distance  = int(round(outputLine[1]))
              threshold = query[1]
              ind_hard  = int(distance < threshold)
              ind_soft = 1 - 1.0 / (1+np.exp(-soft_threshold_slope*(distance-threshold)/threshold))
              place = "before chunk append"
              chunkedLines.append("('{ID_A}','{ID_B}','{query}',{distance},{threshold},{ind_hard},{ind_soft})".format(ID_A      = ID_A,
                                                                                                                      ID_B      = ID_B,
                                                                                                                      query     = query[0],
                                                                                                                      distance  = distance,
                                                                                                                      threshold = threshold,
                                                                                                                      ind_hard  = ind_hard,
                                                                                                                      ind_soft  = ind_soft
                                                                                                                      ))
              if(count % sqlChunkify == 0):
                curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+' ON CONFLICT DO NOTHING')
                conn.commit()
                chunkedLines = list()
            if(count % sqlChunkify != 0):
              curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+' ON CONFLICT DO NOTHING')
              conn.commit()
            
            writeLog(hex,A_pointCount,dest_name,"Solved",(time.time()-hexStartTime)/60)
    curs.execute("SELECT COUNT(*) FROM {}".format(sqlTableName))
    numerator = list(curs)
    numerator = int(numerator[0][0])
    progressor(numerator,denominator,start,"{}/{}; last hex processed: {}, at {}".format(numerator,denominator,hex,time.strftime("%Y%m%d-%H%M%S"))) 
  except:
    print('''Error: {}
             Place: {}
      '''.format( sys.exc_info(),place))   

  finally:
    arcpy.CheckInExtension('Network')
    conn.close()



# get list of hexes over which to iterate
curs.execute("SELECT hex FROM hex_parcels;")
hex_list = list(curs)    

# MAIN PROCESS
if __name__ == '__main__':
  try:
    conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
    curs = conn.cursor()
    
    # create OD matrix table (Closest POS)
    curs.execute(createTable)
    conn.commit()
    
  except:
    print("SQL connection error")
    print(sys.exc_info())
    raise
    
  # initiate log file
  writeLog(create='create')  
  
  # Setup a pool of workers/child processes and split log output
  nWorkers = 4  
  pool = multiprocessing.Pool(nWorkers)
    
  # Task name is now defined
  task = 'Create OD cost matrix for parcel points to closest POS (any size)'  # Do stuff
  print("Commencing task ({}): {} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))

  # Divide work by hexes
  # Note: if a restricted list of hexes are wished to be processed, just supply a subset of hex_list including only the relevant hex id numbers.
  iteration_list = np.asarray([x[0] for x in hex_list])
  pool.map(ODMatrixWorkerFunction, iteration_list, chunksize=1)
  	
    
  curs.execute(primary_key)
  conn.commit()
  
  # output to completion log    
  script_running_log(script, task, start, locale)
  conn.close()