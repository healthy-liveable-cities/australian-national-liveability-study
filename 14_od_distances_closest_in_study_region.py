# Script:  createODmatrix_Loop_parallelised_closestAB.py
# Purpose: This script finds for each A point the closest B point along a network.
#              - it uses parallel processing
#              - it outputs to an sql database 
# Authors: Carl Higgs, Koen Simons
#
# Note: Following processing, I would recommend you check out the progress_od_closest table 
# in postgresql and consider the entries with 'no solution' - are these reasonable?
# For example - in psql run query 
# SELECT * FROM progress_od_closest WHERE status = 'no solution' ORDER BY random() limit 20;
# Now, using ArcMap check out those polygones and destinations - can you explain why there 
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
from _project_setup import *

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

## specify "destination_points" (e.g. destinations)
destination_pointsID = destination_id

# Get a list of feature 
featureClasses = arcpy.ListFeatureClasses()

# Processing is undertake for any value > polygonStart
# So, if you want to start from a specific polygon number,
# you could change this to a larger value
polygonStart = 0

# SQL Settings
progress_table    = "progress_od_closest"

sqlChunkify = 500
  
# initiate postgresql connection
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

# define reduced set of destinations and cutoffs (ie. only those with cutoffs defined)
sql = '''
    SELECT dest_class,
           cutoff_closest 
      FROM dest_type 
     WHERE cutoff_closest IS NOT NULL
       AND count > 0
       AND dest_class IN ('{}');
       '''.format("','".join(ad_hoc_destinations))
curs.execute(sql)
destination_list = list(curs)

# tally expected parcel-destination class result set  
curs.execute("SELECT COUNT(*) FROM {sample_point_feature};".format(sample_point_feature = sample_point_feature))
sample_point_count = list(curs)[0][0]
completion_goal = sample_point_count * len(destination_list)
conn.close()
# get pid name
pid = multiprocessing.current_process().name

# Worker/Child PROCESS
def ODMatrixWorkerFunction(polygon): 
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
 
  # Worker Task is polygon-specific by definition/parallel
  # Skip if polygon was finished in previous run
  polygonStartTime = time.time()
  if polygon < polygonStart:
    return(1)
    
  try:    
    # identify any points from this polygon without a sausage buffer; lets not consider them
    sql = '''SELECT {points_id} FROM no_sausage WHERE {polygon_id} = {polygon}'''.format(polygon_id = polygon_id,polygon = polygon)
    curs.execute(sql)
    if len(list(curs)) > 0:
      exclude_points = '''AND {points_id} NOT IN ('{exclude}')'''.format(points_id = points_id,
                                                                  exclude = ','.join([x[0] for x in list(curs)]))
    # select origin points 
    arcpy.MakeFeatureLayer_management (sample_point_feature, "sample_point_feature_layer")
    origin_selection = arcpy.SelectLayerByAttribute_management("sample_point_feature_layer", 
                          where_clause = '{polygon_id} = {polygon} {exclude_points}'.format(polygon_id = polygon_id,
                                                                                               polygon = polygon,
                                                                                     exclude_points= exclude_points))
    origin_point_count = int(arcpy.GetCount_management(origin_selection).getOutput(0))
    # Skip polygones with zero adresses
    if origin_point_count == 0:
        writeLog(polygon,0,'NULL',"no valid origin points",(time.time()-polygonStartTime)/60)
        return(2)
    
    # make destination feature layer
    arcpy.MakeFeatureLayer_management (outCombinedFeature, "destination_points_layer")        
    
    # fetch list of successfully processed destinations for this polygon, if any
    curs.execute("SELECT dest_class FROM {} WHERE polygon = {}".format(progress_table,polygon))
    completed_dest = [x[0] for x in list(curs)]
    remaining_dest_list = [x for x in destination_list if x[0] not in completed_dest]
    
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
    
    for destination_points in remaining_dest_list:
      dest_class = destination_points[0]
      dest_class = destination_points[1]
      dest_cutoff_threshold = destination_points[2]
      destStartTime = time.time()
      # select destination points 
      destination_selection = arcpy.SelectLayerByAttribute_management("destination_points_layer", where_clause = "dest_class = '{}'".format(dest_class))
      # OD Matrix Setup
      arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
          sub_layer                      = originsLayerName, 
          in_table                       = origin_selection, 
          field_mappings                 = "Name {} #".format(points_id), 
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
        writeLog(polygon,origin_point_count,dest_class,"no solution",(time.time()-destStartTime)/60)
      else:
        # Extract lines layer, export to SQL database
        outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)
        count = 0
        chunkedLines = list()
        for outputLine in outputLines :
          count += 1
          origin_id      = outputLine[0].split('-')[0].strip(' ')
          dest_id   = outputLine[0].split('-')[1].split(',')
          dest_class = dest_id[0].strip(' ')
          dest_id   = dest_id[1].strip(' ')
          distance  = int(round(outputLine[1]))
          threshold = float(dest_cutoff_threshold)
          ind_hard  = int(distance < threshold)
          ind_soft = 1 - 1.0 / (1+np.exp(-soft_threshold_slope*(distance-threshold)/threshold))
          chunkedLines.append('''('{point_id}','{d_class}','{d_name}',{d_id},{distance},{threshold},{ind_h},{ind_s})'''.format(point_id  = origin_id,
                                                                                                                               d_class = dest_class,
                                                                                                                               d_name = dest_class,
                                                                                                                               d_id   = dest_id,
                                                                                                                               distance  = distance,
                                                                                                                               threshold = threshold,
                                                                                                                               ind_h  = ind_hard,
                                                                                                                               ind_s  = ind_soft))
          if(count % sqlChunkify == 0):
            sql = '''
            INSERT INTO {result_table} AS o VALUES {values} 
            ON CONFLICT ({points_id},dest_class) 
            DO UPDATE 
            SET dest_class = EXCLUDED.dest_class,
                oid       = EXCLUDED.oid,
                distance  = EXCLUDED.distance,
            WHERE EXCLUDED.distance < o.distance;
            '''.format(result_table=result_table, 
                        values = ','.join(chunkedLines),
                        points_id = points_id)
            curs.execute(sql)
            conn.commit()
            chunkedLines = list()
        if(count % sqlChunkify != 0):
          sql = '''
          INSERT INTO {result_table} AS o VALUES {values} 
          ON CONFLICT ({points_id}) 
          DO UPDATE 
          SET distances  = array_append_if_gr(o.distances,EXCLUDED.distance),
          WHERE o.distance = '{}'::int[];/
          '''.format(result_table=result_table, 
                      values = ','.join(chunkedLines),
                      points_id = points_id)
          curs.execute(sql)
          conn.commit()
        writeLog(polygon,origin_point_count,dest_class,"Solved",(time.time()-destStartTime)/60)
    # return worker function as completed once all destinations processed
    return 0
  except:
    print(sys.exc_info())

  finally:
    arcpy.CheckInExtension('Network')
    # Report on progress
    curs.execute("SELECT count(*) FROM od_closest WHERE dest_class IN (SELECT dest_class FROM dest_type);".format(progress_table))
    progress = int(list(curs)[0][0]) 
    progressor(progress,completion_goal,start,"{numerator} / {denominator} parcel-destination combinations processed.".format(numerator = progress,denominator = completion_goal))
    # Close SQL connection
    conn.close()


    
# MAIN PROCESS
if __name__ == '__main__':
  # Task name is now defined
  task = 'Find closest of each destination type to origin'  # Do stuff
  print("Commencing task ({}): {} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))
  # print('''
  # Please note that this script assumes sausage buffers have previously been calculated, 
  # drawing upon the 'no_sausage' table to exclude any points listed there from consideration.
  # ''')
  try:
    conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
    curs = conn.cursor()
  except:
    print("SQL connection error")
    print(sys.exc_info()[0])
    raise
  
  print("Create a table for tracking progress... "), 
  sql = '''
    DROP TABLE IF EXISTS {progress_table};
    CREATE TABLE IF NOT EXISTS {progress_table} AS SELECT 0 AS processed;
    '''.format(progress_table = progress_table)
  # print(create_progress_table)
  curs.execute(sql)
  conn.commit()
  print("Done.")
  sql = '''
   SELECT destinations.count * parcels.count, 
          destinations.count, 
          parcels.count
   FROM (SELECT COUNT(DISTINCT(dest_class)) FROM dest_type WHERE cutoff_count IS NOT NULL and count > 0) destinations,
        (SELECT COUNT(*) FROM {sample_point_feature}) parcels;
  '''.format(sample_point_feature=sample_point_feature)
  curs.execute(sql)
  results = list(curs)[0]
  goal = results[0]
  destinations = results[1]
  parcels = results[2]  
  print("Commence multiprocessing..."),
  # Parallel processing setting
  pool = multiprocessing.Pool(processes=nWorkers)
  # get list of polygons over which to iterate
  sql = '''
    SELECT {polygon_id} 
      FROM {polygon_feature};
      '''.format(polygon_id=polygon_id,
                 polygon_feature=polygon_feature)
  curs.execute(sql)
  iteration_list = np.asarray([x[0] for x in list(curs)])
  # # Iterate process over polygones across nWorkers
  pool.map(ODMatrixWorkerFunction, iteration_list, chunksize=1)
  
  # output to completion log    
  conn.close()
  script_running_log(script, task, start, locale)
