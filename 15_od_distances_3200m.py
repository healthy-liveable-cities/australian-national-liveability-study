# Script:  15_od_distances_3200m.py
# Purpose: This script records the distances to all destinations within 3200m
# Authors: Carl Higgs
# Date: 20190208

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
result_table = "od_distances_3200m"
progress_table = "{table}_progress".format(table = result_table)

# SQL insert queries
insert1 = '''INSERT INTO {table} ({id}, hex, dest_class, distances) SELECT {id},'''.format(table = result_table,
                                                                                       id = origin_pointsID.lower())
insert2 = ''' AS hex, dest_class, array_agg(distance) AS distances FROM (VALUES '''     
insert3 = ''') v({id}, dest_class, distance) GROUP BY {id}, dest_class '''.format(id = origin_pointsID.lower()) 
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
  # initial postgresql connection
  conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
  curs = conn.cursor()  
  
  # define reduced set of destinations and cutoffs (ie. only those with cutoffs defined)
  curs.execute("SELECT DISTINCT(dest_class) FROM dest_type WHERE cutoff_count IS NOT NULL AND count > 0;")
  destination_list = list(curs)
  
  # tally expected parcel-destination class result set  
  curs.execute("SELECT COUNT(*) FROM parcel_dwellings;")
  completion_goal = list(curs)[0][0] * len(set([x[0] for x in destination_list]))
  conn.close()

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
    # select origin points    
    origin_selection = arcpy.SelectLayerByAttribute_management("origin_points_layer", where_clause = 'HEX_ID = {}'.format(hex))
    origin_point_count = int(arcpy.GetCount_management(origin_selection).getOutput(0))
    # Skip hexes with zero adresses
    if origin_point_count == 0:
        return(2)
    # fetch list of successfully processed destinations for this hex, if any
    curs.execute('''SELECT dest_class 
                    FROM (SELECT DISTINCT(dest_class),
                                 COUNT(*)
                            FROM {table}
                           WHERE hex = {hex}
                           GROUP BY dest_class) t
                    WHERE count = (SELECT parcel_count FROM hex_parcels WHERE hex = {hex});'''.format(table = result_table, hex = hex))
    curs.execute('''SELECT DISTINCT(dest_class),
                                 COUNT(*)
                            FROM {table}
                           WHERE hex = {hex}
                           GROUP BY dest_class;'''.format(table = result_table, hex = hex))
    hex_dests = list(curs)
    completed_dest = [x[0] for x in hex_dests if x[1] == origin_point_count]
    remaining_dest_list = [x for x in hex_dests if x[0] in [d[0] for d in destination_list] and x[0] not in completed_dest]
    not_processed_dest = [(x[0],0) for x in destination_list if x[0] not in [d[0] for d in hex_dests]][0]
    remaining_dest_list.append(not_processed_dest)
    if len(remaining_dest_list) == 0:
        return(0)
    place = 'before hex selection'
    hex_selection = arcpy.SelectLayerByAttribute_management("hex_layer", where_clause = 'OBJECTID = {}'.format(hex))
    place = 'before destination in hex selection'
    dest_in_hex = arcpy.SelectLayerByLocation_management("destination_points_layer", 'WITHIN_A_DISTANCE',hex_selection,3200)
    dest_in_hex_count = int(arcpy.GetCount_management(dest_in_hex).getOutput(0))
    if dest_in_hex_count == 0: 
        place = 'zero dest in hex, so insert null records'
        # print(place)
        null_dest_insert = '''
         INSERT INTO {table} ({id}, hex, dest_class, distances)  
         SELECT gnaf_pid,{hex}, dest_class, '{curlyo}{curlyc}'::int[] 
           FROM parcel_dwellings 
         CROSS JOIN (SELECT DISTINCT(dest_class) dest_class 
                       FROM dest_type 
                      WHERE dest_class IN ('{dest_list}')) d
          WHERE hex_id = {hex};
         '''.format(table = result_table,
                    id = origin_pointsID.lower(), 
                    hex = hex,
                    curlyo = '{',
                    curlyc = '}',
                    dest_list = "','".join([x[0] for x in remaining_dest_list]))
        # print(null_dest_insert)                    
        curs.execute(null_dest_insert)
        conn.commit()
        # update current progress
        place = "update progress (zero destinations)"
        curs.execute('''UPDATE {progress_table} SET processed = processed+{count}'''.format(progress_table = progress_table,
                                                                                             count = origin_point_count * len(remaining_dest_list)))
        conn.commit() 
    else: 
        # We now know there are destinations to be processed remaining in this hex, so we proceed
        for dest_class in remaining_dest_list:
            dest_count = dest_class[1]
            origin_dest_point_count = origin_point_count - dest_count
            dest_class = dest_class[0]
            destStartTime = time.time()
            if dest_count > 0:
              curs.execute('''SELECT {id} 
                              FROM parcel_dwellings p 
                              WHERE hex_id = {hex}
                              AND NOT EXISTS (SELECT 1 FROM {table} o 
                                              WHERE hex = {hex}
                                                AND dest_class = '{dest_class}' 
                                                AND p.{id} = o.{id});
                           '''.format(table = result_table,
                                      id = origin_pointsID.lower(), 
                                      hex = hex,
                                      dest_class = dest_class)
              remaining_parcels = [x[0] for x in list(curs)]
              origin_subset = arcpy.SelectLayerByAttribute_management("origin_points_layer", 
                                                                      where_clause = 'HEX_ID = {} AND {id} IN ('{}')'.format(hex,
                                                                                                                             "','".join(remaining_parcels)))
              # OD Matrix Setup      
              arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
                  sub_layer                      = originsLayerName, 
                  in_table                       = origin_subset, 
                  field_mappings                 = "Name {} #".format(origin_pointsID), 
                  search_tolerance               = "{} Meters".format(tolerance), 
                  search_criteria                = "{} SHAPE;{} NONE".format(network_edges,network_junctions), 
                  append                         = "CLEAR", 
                  snap_to_position_along_network = "NO_SNAP", 
                  exclude_restricted_elements    = "INCLUDE",
                  search_query                   = "{} #;{} #".format(network_edges,network_junctions))
            else:
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

            # select destination points 
            destination_selection = arcpy.SelectLayerByAttribute_management(dest_in_hex, where_clause = "dest_class = '{}'".format(dest_class))
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
              place = 'OD results processed, but no results recorded'
              null_dest_insert = '''
                INSERT INTO {table} ({id}, hex, dest_class, distances)  
                SELECT gnaf_pid,{hex}, '{dest_class}', '{curlyo}{curlyc}'::int[] 
                  FROM parcel_dwellings 
                 WHERE hex_id = {hex} 
                 ON CONFLICT DO NOTHING;
                '''.format(table = result_table,
                           id = origin_pointsID.lower(), 
                           hex = hex,
                           curlyo = '{',
                           curlyc = '}',
                           dest_class = dest_class) 
              # print(null_dest_insert)                           
              curs.execute(null_dest_insert)
              conn.commit()
              place = "update progress (post OD matrix results, no results)"
            else:
              place = 'results were returned, now processing...'
              # Extract lines layer, export to SQL database
              outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)        
              chunkedLines = list()
              # Result table queries
              place = 'before outputLine loop'
              for outputLine in outputLines:
                origin_id      = outputLine[0].split('-')[0].strip(' ')
                dest_id   = outputLine[0].split('-')[1].split(',')
                dest_class = dest_id[0].strip(' ')
                distance  = int(round(outputLine[1]))
                place = "before chunk append of returned and processed results"
                chunkedLines.append('''('{origin_id}','{dest_class}',{distance})'''.format(origin_id = origin_id,
                                                                                          dest_class = dest_class,
                                                                                          distance  = distance))
              place = "before execute returned results sql"
              sql_query = '''{insert1}{hex}{insert2}{values}{insert3}{insert4}'''.format(insert1 = insert1,
                                                                                          hex = hex,
                                                                                          insert2 = insert2,
                                                                                          values   = ','.join(chunkedLines),
                                                                                          insert3 = insert3,
                                                                                          insert4 = insert4)
              curs.execute(sql_query)
              place = "before commit of returned and processed results"
              conn.commit()
              # Where results don't exist for a destination class, ensure a null array is recorded
              null_dest_insert = '''
               INSERT INTO {table} ({id}, hex, dest_class, distances)  
               SELECT gnaf_pid,{hex}, '{dest_class}', '{curlyo}{curlyc}'::int[] 
                 FROM parcel_dwellings p
               WHERE hex_id = {hex}
                 AND NOT EXISTS (SELECT 1 FROM {table} o 
                                  WHERE dest_class = '{dest_class}' 
                                    AND hex = {hex}
                                    AND p.{id} = o.{id});
               '''.format(table = result_table,
                          id = origin_pointsID.lower(), 
                          hex = hex,
                          curlyo = '{',
                          curlyc = '}',
                          dest_class = dest_class)   
              # print(null_dest_insert)                          
              curs.execute(null_dest_insert)
              conn.commit()       
              place = "update progress (post OD matrix results, successful)"
            # update current progress
            curs.execute('''UPDATE {progress_table} SET processed = processed+{count}'''.format(progress_table = progress_table,
                                                                                                 count = origin_dest_point_count))
            conn.commit()
    curs.execute('''SELECT processed from {progress_table}'''.format(progress_table = progress_table))
    progress = int(list(curs)[0][0])
    place = 'final progress'
    progressor(progress,
               completion_goal,
               start,
               '''{}/{}; last hex processed: {}, at {}'''.format(progress,
                                                                 completion_goal,
                                                                 hex,
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
  # initial postgresql connection
  conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
  curs = conn.cursor()  
  
  create_results_table = '''
  --DROP TABLE IF EXISTS {0};
  CREATE TABLE IF NOT EXISTS {0}
  ({1} varchar NOT NULL ,
   hex integer NOT NULL, 
   dest_class varchar NOT NULL ,
   distances int[] NOT NULL, 
   PRIMARY KEY({1},dest_class)
   );
   '''.format(result_table, origin_pointsID)
  curs.execute(create_results_table)
  conn.commit()

  print("Create a table for tracking progress... "), 
  create_progress_table = '''
    DROP TABLE IF EXISTS {progress_table};
    CREATE TABLE IF NOT EXISTS {progress_table} (processed int);
    INSERT INTO {progress_table} SELECT count(*) processed FROM {result_table};
    '''.format(progress_table = progress_table,
               result_table = result_table)
  # print(create_progress_table)
  curs.execute(create_progress_table)
  conn.commit()
  print("Done.")
  evaluate_progress = '''
   SELECT destinations.count * parcels.count, 
          destinations.count, 
          parcels.count, 
          processed.processed
   FROM (SELECT COUNT(DISTINCT(dest_class)) FROM dest_type WHERE cutoff_count IS NOT NULL and count > 0) destinations,
        (SELECT COUNT(*) FROM parcel_dwellings) parcels,
        (SELECT processed FROM od_distances_3200m_progress) processed;
  '''
  curs.execute(evaluate_progress)
  results = list(curs)[0]
  goal = results[0]
  destinations = results[1]
  parcels = results[2]
  processed = results[3]
  if processed < goal:
    print("Commence multiprocessing..."),
    # Parallel processing setting
    # (now set as parameter in ind_study_region_matrix xlsx file)
    pool = multiprocessing.Pool(processes=nWorkers)
    # get list of hexes over which to iterate
    curs.execute("SELECT hex FROM hex_parcels;")
    iteration_list = np.asarray([x[0] for x in list(curs)])
    # # Iterate process over hexes across nWorkers
    pool.map(ODMatrixWorkerFunction, iteration_list, chunksize=1)
    evaluate_progress = '''
     SELECT destinations.count * parcels.count, 
            destinations.count, 
            parcels.count, 
            processed.processed
     FROM (SELECT COUNT(DISTINCT(dest_class)) FROM dest_type WHERE cutoff_count IS NOT NULL and count > 0) destinations,
          (SELECT COUNT(*) FROM parcel_dwellings) parcels,
          (SELECT processed FROM od_distances_3200m_progress) processed;
    '''
    curs.execute(evaluate_progress)
    results = list(curs)[0]
    goal = results[0]
    destinations = results[1]
    parcels = results[2]
    processed = results[3]
    if processed < goal:
      print('''The script has finished running, however the number of results processed {} is still less than the goal{}.  There may be a bug, so please investigate how this has occurred in more depth.'''.format(processed,goal))
  else: 
    print('''It appears that {} destinations have already been processed for {} parcels, yielding {} results.'''.format(destinations,
                                                                                                                        parcels,
                                                                                                                        processed))
    if processed > goal:
      print('''The number of processed results is larger than the completion goal however ({})'''.format(goal))
      print('''So it appears something has gone wrong. Please check how this might have occurred.  There may be a bug.''')
    else:
      print('''That appears to be equal to the completion goal of {} results; so all good!  It looks like this script is done.'''.format(goal))
  # output to completion log    
  if processed == goal:
    # this script will only be marked as successfully complete if the number of results processed matches the completion goal.
    script_running_log(script, task, start, locale)
  conn.close()
