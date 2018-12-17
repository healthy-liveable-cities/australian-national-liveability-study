# Script:  09_od_count_in_buffer_distance.py
# Purpose: This script counts the number of destinations within a threshold distance
# Authors: Carl Higgs, Koen Simons
# Date: 20180707

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

sqlChunkify = 500
  
# initiate postgresql connection
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

# define reduced set of destinations and cutoffs (ie. only those with cutoffs defined)
curs.execute("SELECT dest_name,dest_class,cutoff_count FROM dest_type WHERE cutoff_count IS NOT NULL AND count > 0;")
destination_list = list(curs)

# tally expected parcel-destination class result set  
curs.execute("SELECT COUNT(*) FROM parcel_dwellings;")
completion_goal = list(curs)[0][0] * len(set([x[1] for x in destination_list]))

# get pid name
pid = multiprocessing.current_process().name

createTable_log     = '''
    --DROP TABLE IF EXISTS {0};
    CREATE TABLE IF NOT EXISTS {0}
      (hex integer NOT NULL, 
      parcel_count integer NOT NULL, 
      dest_name varchar, 
      status varchar, 
      mins double precision,
      PRIMARY KEY(hex,dest_name)
      );
          '''.format(log_table)    

logInsert      = '''
  INSERT INTO {} VALUES
  '''.format(log_table)          

logUpdate      = '''
  ON CONFLICT ({0},{4}) 
  DO UPDATE SET {1}=EXCLUDED.{1},{2}=EXCLUDED.{2},{3}=EXCLUDED.{3}
  '''.format('hex','parcel_count','status','mins','dest_name')            
    
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
      curs.execute("{0} ({1},{2},'{3}','{4}',{5}) {6}".format(logInsert,hex, AhexN, Bcode,status, mins, logUpdate))
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

    # fetch list of successfully processed destinations for this hex, if any
    curs.execute("SELECT dest_name FROM {} WHERE hex = {}".format(log_table,hex))
    completed_dest = [x[0] for x in list(curs)]
    remaining_dest_list = [x for x in destination_list if x[0] not in completed_dest]
    
    for destination_points in remaining_dest_list:
      destStartTime = time.time()
      dest_name = destination_points[0]
      dest_class = destination_points[1]
      threshold = destination_points[2]
      chunkedLines = list()
      # select destination points 
      destination_selection = arcpy.SelectLayerByAttribute_management("destination_points_layer", where_clause = "dest_name = '{}'".format(dest_name))
      # OD Matrix Setup
      
      # Make OD cost matrix layer
      result_object = arcpy.MakeODCostMatrixLayer_na(in_network_dataset = in_network_dataset, 
                                                     out_network_analysis_layer = "ODmatrix", 
                                                     impedance_attribute = "Length",  
                                                     default_cutoff = threshold,
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
        # If no results for this hex-destination combination, we record these zero counts
        no_result_query = '''
        INSERT INTO {od_distances} AS o ({id},dest_class,dest_name,cutoff,count)
        SELECT {id}, '{dest_class}','{s}{dest_name}{e}',{threshold},0 
        FROM parcel_dwellings p
        WHERE NOT EXISTS
        (SELECT 1 FROM {od_distances} o 
          WHERE o.{id}=p.{id}  AND o.dest_name @> '{s}{dest_name}{e}' AND p.hex_id = {hex})
        AND hex_id = {hex}
        ON CONFLICT ({id},dest_class) 
        DO UPDATE SET dest_name = o.dest_name || EXCLUDED.dest_name,
        count  = o.count+EXCLUDED.count
        WHERE NOT EXCLUDED.dest_name <@ o.dest_name;
        ;
        '''.format(od_distances = od_distances,
                   dest_class = dest_class, 
                   s = '{',
                   dest_name = dest_name, 
                   e = '}',
                   threshold=threshold,
                   id = origin_pointsID,
                   hex = hex)
        curs.execute(no_result_query)
        conn.commit()
        writeLog(hex,origin_point_count,dest_name,"none found",(time.time()-destStartTime)/60)
      else:
        # get dest_class for feature
        # Extract lines layer, export to SQL database
        df = arcpy.da.TableToNumPyArray(ODLinesSubLayer, 'Name')    
        stripped_df = [f[0].encode('utf-8').split(' - ')[0] for f in df]
        id_counts = np.unique(stripped_df, return_counts=True)
        length  = len(id_counts[0])-1
        count = 0
        place = "before loop"
        for x in range(0,length) :
          count += 1
          point_id = id_counts[0][x]
          tally = id_counts[1][x]
          chunkedLines.append('''('{point_id}','{dest_class}','{s}{dest_name}{e}',{threshold},{tally})'''.format(point_id=point_id,
                                                                                                           dest_class=dest_class,
                                                                                                           s = '{',
                                                                                                           dest_name=dest_name,
                                                                                                           e= '}',
                                                                                                           threshold=threshold,
                                                                                                           tally=tally))
          if(count % sqlChunkify == 0):
            place = "before postgresql out"
            sql = '''
            INSERT INTO {od_distances} AS o VALUES {values} 
            ON CONFLICT ({id},dest_class) 
            DO UPDATE SET dest_name = o.dest_name || EXCLUDED.dest_name,
            count  = o.count+EXCLUDED.count
            WHERE NOT EXCLUDED.dest_name <@ o.dest_name;
            '''.format(od_distances=od_distances, 
                        values = ','.join(chunkedLines),
                        id = origin_pointsID)
            curs.execute(sql)
            conn.commit()
            chunkedLines = list() 
        if(count % sqlChunkify != 0):
          sql = '''
          INSERT INTO {od_distances} AS o VALUES {values} 
          ON CONFLICT ({id},dest_class) 
          DO UPDATE SET dest_name = o.dest_name || EXCLUDED.dest_name,
          count  = o.count+EXCLUDED.count
          WHERE NOT EXCLUDED.dest_name <@ o.dest_name;
          '''.format(od_distances=od_distances, 
                      values = ','.join(chunkedLines),
                      id = origin_pointsID)
          curs.execute(sql)
          conn.commit()
        # If no results for this hex-destination combination, we record these zero counts
        no_result_query = '''
        INSERT INTO {od_distances} AS o ({id},dest_class,dest_name,cutoff,count)
        SELECT {id}, '{dest_class}','{s}{dest_name}{e}',{threshold},0 
        FROM parcel_dwellings p
        WHERE NOT EXISTS
        (SELECT 1 FROM {od_distances} o 
          WHERE o.{id}=p.{id}  AND o.dest_name @> '{s}{dest_name}{e}' AND p.hex_id = {hex})
        AND hex_id = {hex}
        ON CONFLICT ({id},dest_class) 
        DO UPDATE SET dest_name = o.dest_name || EXCLUDED.dest_name,
        count  = o.count+EXCLUDED.count
        WHERE NOT EXCLUDED.dest_name <@ o.dest_name;
        ;
        '''.format(od_distances = od_distances,
                   dest_class = dest_class, 
                   s = '{',
                   dest_name = dest_name, 
                   e = '}',
                   threshold=threshold,
                   id = origin_pointsID,
                   hex = hex)
        curs.execute(no_result_query)
        conn.commit()
        writeLog(hex,origin_point_count,dest_name,"Solved",(time.time()-destStartTime)/60)
    # return worker function as completed once all destinations processed
    return 0
  
  except:
    print(sys.exc_info())
    print(chunkedLines)
    
  finally:
    arcpy.CheckInExtension('Network')
    # Report on progress
    curs.execute('''SELECT count(*) FROM od_counts WHERE dest_name IN (SELECT dest_name FROM dest_type);''')
    progress = int(list(curs)[0][0]) 
    progressor(progress,completion_goal,start,"{numerator} / {denominator} combinations processed.".format(numerator = progress,denominator = completion_goal))
    # Close SQL connection
    conn.close()

    
# MAIN PROCESS
if __name__ == '__main__':
  # Task name is now defined
  task = 'Count destinations within network buffer distance of origins'  # Do stuff
  print("Commencing task ({}): {} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))

  # Define query to create table
  createTable     = '''
  --DROP TABLE IF EXISTS {0};
  CREATE TABLE IF NOT EXISTS {0}
  ({1} varchar NOT NULL ,
   dest_class varchar NOT NULL ,
   dest_name varchar[] NOT NULL ,
   cutoff integer NOT NULL, 
   count integer NOT NULL, 
   PRIMARY KEY({1},dest_class)
   );
   '''.format(od_distances, origin_pointsID)

  # create OD matrix table
  curs.execute(createTable)
  conn.commit()
   
  print("Initialise log file..."),
  writeLog(create='create')
  print(" Done.")
  
  print("Setup a pool of workers/child processes and split log output..."),
  # Parallel processing setting
  # (now set as parameter in ind_study_region_matrix xlsx file)
  # nWorkers = 4  
  pool = multiprocessing.Pool(processes=nWorkers)
  print(" Done.")

  print("Iterate over hexes...")
  # get list of hexes over which to iterate
  curs.execute("SELECT hex FROM hex_parcels;")
  hex_list = list(curs)   
  iteration_list = np.asarray([x[0] for x in hex_list])
  # # Iterate process over hexes across nWorkers
  pool.map(ODMatrixWorkerFunction, iteration_list, chunksize=1)
  
  # output to completion log    
  script_running_log(script, task, start, locale)
  conn.close()
