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

# SQL Settings
## Note - this used to be 'dist_cl_od_parcel_dest' --- simplified to 'result_table'
result_table = "od_counts_3200"

sqlChunkify = 500
  
# initiate postgresql connection
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

# define reduced set of destinations and cutoffs (ie. only those with cutoffs defined)
curs.execute("SELECT DISTINCT(dest_class) FROM dest_type WHERE cutoff_count IS NOT NULL AND count > 0;")
destination_list = list(curs)

# tally expected parcel-destination class result set  
curs.execute("SELECT COUNT(*) FROM parcel_dwellings;")
completion_goal = list(curs)[0][0] * len(set([x[0] for x in destination_list]))

# SQL insert queries
insert1 = '''INSERT INTO {table} ({id}, dest_class, distances) SELECT {id},'''.format(table = result_table,
                                                                                       id = origin_pointsID.lower())
insert2 = ''', dest_class, array_agg(distance) AS distances FROM (VALUES '''     
insert3 = ''') v({id}, dest_class, distance) GROUP BY {id}, dest_class, hex ''' 
insert4 = '''ON CONFLICT DO NOTHING;'''

# get pid name
pid = multiprocessing.current_process().name

if pid !='MainProcess':
  # Make OD cost matrix layer
  result_object = arcpy.MakeODCostMatrixLayer_na(in_network_dataset = in_network_dataset, 
                                                 out_network_analysis_layer = "ODmatrix", 
                                                 impedance_attribute = "Length", 
                                                 default_cutoff = 3200,
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
  arcpy.MakeFeatureLayer_management (origin_points, "origin_points_layer")
  arcpy.MakeFeatureLayer_management (outCombinedFeature, "destination_points_layer")       
  arcpy.MakeFeatureLayer_management(hex_grid, "hex_layer")   

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
  try:    
    output_values = list()
    count = 0
    # select origin points    
    origin_selection = arcpy.SelectLayerByAttribute_management("origin_points_layer", where_clause = 'HEX_ID = {}'.format(hex))
    origin_point_count = int(arcpy.GetCount_management(origin_selection).getOutput(0))
    # Skip hexes with zero adresses
    if origin_point_count == 0:
        return(2)
    # fetch list of successfully processed destinations for this hex, if any
    curs.execute('''SELECT DISTINCT(dest_class)
                      FROM {table} d
                     WHERE hex = {hex}'''.format(table = result_table, hex = hex))
    completed_dest = [x[0] for x in list(curs)]
    remaining_dest_list = [x[0] for x in destination_list if x[0] not in completed_dest]
    if len(remaining_dest_list) == 0:
        return(0)
    place = 'before hex selection'
    hex_selection = arcpy.SelectLayerByAttribute_management("hex_layer", where_clause = 'OBJECTID = {}'.format(hex))
    dest_in_hex = arcpy.SelectLayerByLocation_management("destination_points_layer", 'WITHIN_A_DISTANCE',hex_selection,3200)
    dest_in_hex_count = int(arcpy.GetCount_management(dest_in_hex).getOutput(0))
    if dest_in_hex_count == 0: 
        null_dest_insert = '''
         INSERT INTO {table} ({id}, {hex}, dest_class, distances)  
         SELECT gnaf_pid,{hex}, dest_class, '{}'::int[] 
           FROM parcel_dwellings 
         CROSS JOIN (SELECT DISTINCT(dest_class) dest_class 
                       FROM dest_type 
                      WHERE dest_class IN {dest_list}) d
          WHERE hex_id = {hex};
         '''.format(table = result_table,
                    id = origin_pointsID.lower(), 
                    hex = hex[0],
                    dest_list = remaining_dest_list)    
        curs.execute(null_dest_insert)
        conn.commit()
        count += origin_point_count * len(remaining_dest_list)
    else: 
        # We now know there are destinations to be processed remaining in this hex, so we proceed
        for dest_class in remaining_dest_list:
            destStartTime = time.time()
            # select destination points 
            destination_selection = arcpy.SelectLayerByAttribute_management(dest_in_hex, where_clause = "dest_class = '{}'".format(dest_class))
            # OD Matrix Setup      
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
              null_dest_insert = '''
                INSERT INTO {table} ({id}, {hex}, dest_class, distances)  
                SELECT gnaf_pid,{hex}, dest_class, '{}'::int[] 
                  FROM parcel_dwellings 
                CROSS JOIN (SELECT DISTINCT(dest_class) dest_class 
                              FROM dest_type 
                             WHERE dest_class = {dest_class}) d
                 WHERE hex_id = {hex};
                '''.format(table = result_table,
                           id = origin_pointsID.lower(), 
                           hex = hex,
                           dest_class = dest_class)    
              curs.execute(null_dest_insert)
              conn.commit()
            else:
              # Extract lines layer, export to SQL database
              outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)        
              chunkedLines = list()
              # Result table queries
              place = 'before outputLine loop'
              for outputLine in outputLines:
                origin_id      = outputLine[0].split('-')[0].strip(' ')
                dest_class = dest_id[0].strip(' ')
                distance  = int(round(outputLine[1]))
                place = "before chunk append, gnaf = {}".format(pid)
                chunkedLines.append('''('{origin_id}',{dest_class},{distance})'''.format(origin_id = origin_id,
                                                                                          dest_class = dest_class,
                                                                                          distance  = distance))
              place = "before execute sql, gnaf = {}".format(pid)
              curs.execute('''{insert1}{hex}{insert2}{values}{insert3}{insert4}'''.format(insert1 = insert1,
                                                                                          insert2 = insert2,
                                                                                          values   = ','.join(chunkedLines),
                                                                                          insert3 = insert3,
                                                                                          insert4 = insert3))
              place = "before commit, gnaf = {}".format(pid)
              conn.commit()
              # Where results don't exist for a destination class, ensure a null array is recorded
              null_dest_insert = '''
               INSERT INTO {table} ({id}, {hex}, dest_class, distances)  
               SELECT gnaf_pid,{hex}, dest_class, '{}'::int[] 
                 FROM parcel_dwellings 
               WHERE hex = {hex}
                 AND NOT EXISTS (SELECT 1 FROM {table} WHERE dest_class = {dest_class} and hex = {hex});
               '''.format(table = result_table,
                          id = origin_pointsID.lower(), 
                          hex = hex,
                          dest_class = dest_class)    
              curs.execute(null_dest_insert)
              conn.commit()
            count += origin_point_count           
    # update current progress
    curs.execute('''UPDATE {progress_table} SET processed = processed+{count}'''.format(progress_table = progress_table,
                                                                                                 count = A_pointCount))
    conn.commit()
    curs.execute('''SELECT processed from {progress_table}'''.format(progress_table = progress_table))
    progress = int(list(curs)[0][0])
    progressor(progress,
               completion_goal,
               start,
               '''{}/{}; last hex processed: {}, at {}'''.format(progress,
                                                                 completion_goal,
                                                                 hex[0],
                                                                 time.strftime("%Y%m%d-%H%M%S"))) 
  except:
      print('''Error: {}\nPlace: {}'''.format( sys.exc_info(),place))  
  finally:
      arcpy.CheckInExtension('Network')
      conn.close()
    
# MAIN PROCESS
if __name__ == '__main__':
  task = 'Record distances from origins to destinations within 3200m'
  print("Commencing task ({}): {} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))

  create_table = '''
  --DROP TABLE IF EXISTS {0};
  CREATE TABLE IF NOT EXISTS {0}
  ({1} varchar NOT NULL ,
   hex integer NOT NULL, 
   dest_class varchar NOT NULL ,
   distances integer NOT NULL, 
   PRIMARY KEY({1},dest_class)
   );
   '''.format(result_table, origin_pointsID)
  curs.execute(create_table)
  conn.commit()

  print("Create a table for tracking progress... "), 
  od_distances_3200_progress = '''
    DROP TABLE IF EXISTS {table}_progress;
    CREATE TABLE IF NOT EXISTS {table}_progress 
       (processed int);
    '''.format(table = result_table)
  curs.execute(od_distances_3200_progress)
  conn.commit()
  print("Done.")
  
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
