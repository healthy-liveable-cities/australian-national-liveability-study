# Script:  12_od_aos_list_analysis.py
# Purpose: Calcault distance to nearest AOS within 3.2km, 
#          or if none within 3.2km then distance to closest
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
aos_points   =  'aos_nodes_30m_line'
aos_pointsID =  'aos_entryid'

hexStart = 0

# SQL Settings
sqlTableName  = "od_aos"
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
                                                 default_cutoff = aos_threshold,
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
# 'aos_list' is a compound variable -- a list of lists with structure like
# {id, distance, attributes
createTable     = '''
  -- DROP TABLE IF EXISTS {table};
  CREATE TABLE IF NOT EXISTS {table}
  (
  {id} varchar, 
  aos_id INT, 
  node INT, 
  distance INT, 
  query varchar,
  PRIMARY KEY ({id},aos_id) 
  );
   '''.format(table = sqlTableName, id = origin_pointsID.lower())
## LATER index on gnaf_id, query

recInsert      = '''
  INSERT INTO od_aos ({id}, aos_id, node, query, distance) 
  SELECT DISTINCT ON ({id}, aos_id) {id}, aos_id, node, query, min(distance) 
  FROM  ( 
     VALUES
  '''.format(id = origin_pointsID.lower())          
                    
recUpdate      = '''
  ) v({id}, aos_id, node, query, distance) 
   GROUP BY {id},aos_id,node, query 
    ON CONFLICT ({id},aos_id) 
       DO UPDATE 
          SET node = EXCLUDED.node, 
              query = EXCLUDED.query,
              distance = EXCLUDED.distance 
           WHERE od_aos.distance > EXCLUDED.distance;
  '''.format(id = origin_pointsID.lower())  

threshold = 400
calculateThreshold = '''
ALTER TABLE od_aos ADD COLUMN ind_hard int; 
ALTER TABLE od_aos ADD COLUMN ind_soft double precision; 
UPDATE od_aos SET ind_hard = (distance < {threshold})::int; 
UPDATE od_aos SET ind_soft = 1 - 1.0 / (1+exp(-{slope}*(distance-{threshold})/{threshold}::numeric)); 
'''.format(threshold = threshold, slope = soft_threshold_slope)
  
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
  
  
aos_linkage = '''
  -- Associate origin IDs with list of parks 
  DROP TABLE IF EXISTS od_aos_full; 
  CREATE TABLE od_aos_full AS 
  SELECT p.{id}, 
         jsonb_agg(jsonb_strip_nulls(to_jsonb( 
             (SELECT d FROM 
                 (SELECT 
                    p.distance      ,
                    a.gid           ,
                    a.attributes    ,
                    a.numgeom       ,
                    a.aos_ha        ,
                    a.aos_ha_school ,
                    school_os_percent
                    ) d)))) AS attributes 
  FROM od_aos p 
  LEFT JOIN open_space_areas a ON p.aos_id = a.aos_id 
  GROUP BY {id};   
  '''.format(id = origin_pointsID)
  
  
parcel_count = int(arcpy.GetCount_management(origin_points).getOutput(0))  
denominator = parcel_count
 
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
      curs.execute("{0} ({1},{2},$${3}$$,$${4}$$,{5}) {6}".format(queryInsert,hex, AhexN, Bcode,status, mins, queryUpdate))
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
      writeLog(hex,0,"aos","no A points",(time.time()-hexStartTime)/60)
      return(2)
    
    place = 'before buffer selection'
    buffer = arcpy.SelectLayerByAttribute_management("buffer_layer", where_clause = 'ORIG_FID = {}'.format(hex))
    query = 'AOS: in 3.2km'

    # ensure only non-processed parcels are processed (ie. in case script has been previously run)
    curs.execute("SELECT DISTINCT({id}) FROM {table}".format(id = origin_pointsID,
                                                             table = sqlTableName))
    processed_points = [x[0] for x in list(curs)]
    # Only procede with the POS scenario if it has not been previously processed
    if len(processed_points) < A_pointCount:
      arcpy.MakeFeatureLayer_management(aos_points, "aos_pointsLayer")    
      
      # Select and count parks meeting scenario query
      B_selection = arcpy.SelectLayerByLocation_management('aos_pointsLayer', 'intersect', buffer)
      B_pointCount = int(arcpy.GetCount_management(B_selection).getOutput(0))
      
      # define query and distance as destination code, for use in log file 
      dest_name = query

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
          field_mappings                 = "Name {} #".format(aos_pointsID), 
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
            od_pair = outputLine[0].split('-')
            pid = od_pair[0].encode('utf-8').strip(' ')
            aos_pair = od_pair[1].split(',')
            aos = int(aos_pair[0])
            node = int(aos_pair[1])
            distance = int(round(outputLine[1]))
            place = "before chunk append"
            chunkedLines.append("('{pid}',{aos},{node},$${query}$$,{distance})".format(pid = pid,
                                                                                     aos = aos,
                                                                                     node = node,
                                                                                     query     = query,
                                                                                     distance  = distance,
                                                                                         ))
            if(count % sqlChunkify == 0):
              curs.execute(recInsert + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+recUpdate)
              conn.commit()
              chunkedLines = list()
          if(count % sqlChunkify != 0):
            curs.execute(recInsert + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+recUpdate)
            conn.commit()
          
          ### TO DO --- ADD in distance to closest if no results within 3.2km
          writeLog(hex,A_pointCount,dest_name,"Solved",(time.time()-hexStartTime)/60)
    curs.execute("SELECT COUNT(DISTINCT({id})) FROM {table}".format(id = origin_pointsID.lower(),
                                                                    table = sqlTableName))
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
  	
  ## Perhaps add columns for hard and soft thresholds based on 400m at this point?
  #  then, join linkage  
  curs.execute(calculateThreshold)
  conn.commit()
  curs.execute(aos_linkage)
  conn.commit()
  
  # output to completion log    
  script_running_log(script, task, start, locale)
  conn.close()