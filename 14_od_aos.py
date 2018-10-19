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
from sqlalchemy import create_engine
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

# get list of hexes over which to iterate
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))
curs.execute("SELECT sum(parcel_count) FROM hex_parcels;")
total_parcels = int(list(curs)[0][0])
progress_table = 'od_aos_progress'

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
  query varchar,
  distance INT,
  numgeom INT,
  aos_ha numeric,
  aos_ha_total numeric,
  aos_ha_water numeric,
  water_percent numeric, 
  school_os_percent numeric,
  attributes jsonb,  
  PRIMARY KEY ({id},aos_id) 
  );
   '''.format(table = sqlTableName, id = origin_pointsID.lower())
## LATER index on gnaf_id, query

recInsert      = '''
  WITH 
  pre AS 
  (SELECT DISTINCT ON (gnaf_pid, aos_id) gnaf_pid, aos_id, node, query, min(distance) AS distance
   FROM  
   (VALUES 
  '''.format(id = origin_pointsID.lower())          
 
threshold = 400 
recUpdate      = '''
  ) v({id}, aos_id, node, query, distance) 
       GROUP BY {id},aos_id,node,query)
  INSERT INTO {table} ({id}, aos_id, node, query, distance, numgeom, aos_ha, aos_ha_total, aos_ha_water, water_percent, school_os_percent, attributes) 
     SELECT pre.{id}, 
            pre.aos_id, 
            pre.node,
            query,            
            distance,
            numgeom,
            aos_ha,
            aos_ha_total,
            aos_ha_water, 
            water_percent,
            school_os_percent,
            attributes
     FROM pre 
     LEFT JOIN open_space_areas a ON pre.aos_id = a.aos_id
      ON CONFLICT ({id},aos_id) 
         DO UPDATE 
            SET node = EXCLUDED.node, 
                distance = EXCLUDED.distance 
             WHERE {table}.distance > EXCLUDED.distance;
  '''.format(id = origin_pointsID.lower(),
             table = sqlTableName,     
             threshold = threshold,
             slope = soft_threshold_slope)  

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
  SELECT {id}, 
         jsonb_agg(jsonb_strip_nulls(to_jsonb( 
             (SELECT d FROM 
                 (SELECT 
                    distance,
                    aos_id  ,
                    attributes,
                    numgeom   ,
                    aos_ha    ,
                    aos_ha_total ,
                    aos_ha_water, 
                    water_percent,
                    school_os_percent
                    ) d)))) AS attributes 
  FROM od_aos 
  GROUP BY {id};   
  '''.format(id = origin_pointsID)
  
  
parcel_count = int(arcpy.GetCount_management(origin_points).getOutput(0))  
denominator = parcel_count
 
arcpy.MakeFeatureLayer_management(origin_points,"origin_pointsLayer") 
 
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
  # print(hex)
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
  if hex[0] < hexStart:
    return(1)
    
  try:
    count = 0
    to_do_points = hex[1]  
    A_pointCount = len(to_do_points)
    A_selection = arcpy.SelectLayerByAttribute_management("origin_pointsLayer", 
                    where_clause = "hex_id = {hex} AND {id} IN ('{id_list}')".format(hex = hex[0],
                                                                                     id = origin_pointsID,
                                                                                     id_list = "','".join(to_do_points)))   
    # Only procede with the POS scenario if it has not been previously processed
    if len(to_do_points) > 0: 
      A_pointCount = int(arcpy.GetCount_management(A_selection).getOutput(0))    
      place = 'before buffer selection'
      buffer = arcpy.SelectLayerByAttribute_management("buffer_layer", where_clause = 'ORIG_FID = {}'.format(hex[0]))
      query = 'AOS: in {aos_threshold}m'.format(aos_threshold = aos_threshold)
      arcpy.MakeFeatureLayer_management(aos_points, "aos_pointsLayer")  
      
      # Select and count parks meeting scenario query
      B_selection = arcpy.SelectLayerByLocation_management('aos_pointsLayer', 'intersect', buffer)
      B_pointCount = int(arcpy.GetCount_management(B_selection).getOutput(0))
      
      # define query and distance as destination code, for use in log file 
      dest_name = query

      place = 'before skip empty B hexes'
      # Skip empty B hexes
      if B_pointCount > 0:          
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
        ## Comented the following out, as this may not be strictly true in current script form
        ## if result[1] == u'false':
        ##   writeLog(hex[0],A_pointCount,dest_name,"no solution",(time.time()-hexStartTime)/60)
        if result[1] == u'true':
          place = 'After solve'
          # Extract lines layer, export to SQL database
          outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)
          curs = conn.cursor()

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
        writeLog(hex[0],A_pointCount,dest_name,"Solved",(time.time()-hexStartTime)/60)
    curs.execute("UPDATE {progress_table} SET processed = processed+{count}".format(progress_table = progress_table,
                                                                                      count = A_pointCount))
    curs.execute("SELECT processed from {progress_table}".format(progress_table = progress_table))
    progress = int(list(curs)[0][0])
    progressor(progress,total_parcels,start,"{}/{}; last hex processed: {}, at {}".format(progress,total_parcels,hex[0],time.strftime("%Y%m%d-%H%M%S"))) 
  except:
    print('''Error: {}
             Place: {}
      '''.format( sys.exc_info(),place))   

  finally:
    arcpy.CheckInExtension('Network')
    conn.close()

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
  
  # Task name is now defined
  task = 'Create OD cost matrix for parcel points to closest POS (any size)'  # Do stuff
  print("Commencing task ({}):\n{} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))
  # Divide work by hexes
  antijoin = '''
    DROP TABLE IF EXISTS od_aos_hex_todo;
    CREATE TABLE od_aos_hex_todo AS
    SELECT p.hex_id, 
           jsonb_agg(jsonb_strip_nulls(to_jsonb(p.{id}))) AS incomplete
    FROM parcel_dwellings p
    WHERE NOT EXISTS 
    (SELECT 1 FROM od_aos s WHERE s.{id} = p.{id})
    GROUP BY p.hex_id;
  '''.format(id = points_id.lower())
  print("List unprocessed parcels for each hex... "),
  incompletions = pandas.read_sql_query(antijoin,
                                    con=engine)
  print("Done.")

  # Calculated the sum total of parcels that need to be processed, and determine the number already processed
  to_process = incompletions["incomplete"].str.len().sum()
  processed = total_parcels - to_process
  od_aos_progress_table = '''
    DROP TABLE IF EXISTS od_aos_progress;
    CREATE TABLE IF NOT EXISTS od_aos_progress 
       (processed int);
    '''
  print("Create a table for tracking progress... "), 
  curs.execute(od_aos_progress_table)
  conn.commit()
  curs.execute('''INSERT INTO od_aos_progress (processed) VALUES ({})'''.format(processed))
  conn.commit()
  print("Done.")

  to_do_list = incompletions.apply(tuple, axis=1).tolist()
  to_do_list = [[int(x[0]),[p.encode('utf8') for p in x[1]]] for x in to_do_list]
  # iteration_list = np.asarray(to_do_list)
  # Setup a pool of workers/child processes and split log output
  # (now set as parameter in ind_study_region_matrix xlsx file)
  # nWorkers = 4
  print("Commence multiprocessing...")  
  pool = multiprocessing.Pool(nWorkers)
  pool.map(ODMatrixWorkerFunction, to_do_list, chunksize=1)
  	
  print("Aggregate for each parcel address their list of open spaces..."),
  curs.execute(aos_linkage)
  conn.commit()
  print("Done.")
  
  # output to completion log    
  script_running_log(script, task, start, locale)
  conn.close()