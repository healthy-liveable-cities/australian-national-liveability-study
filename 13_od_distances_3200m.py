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
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger
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

# Get a list of feature 
featureClasses = arcpy.ListFeatureClasses()

# SQL Settings
# result_table is now in loop
progress_table = "progress_od_3200m"

# get pid name
pid = multiprocessing.current_process().name

if pid !='MainProcess':
  # Make 3200m OD cost matrix layer
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
  # Make CLOSEST OD cost matrix layer
  cl_result_object = arcpy.MakeODCostMatrixLayer_na(in_network_dataset = in_network_dataset, 
                                                   out_network_analysis_layer = "ODmatrix", 
                                                   impedance_attribute = "Length", 
                                                   default_number_destinations_to_find = 1,
                                                   UTurn_policy = "ALLOW_UTURNS", 
                                                   hierarchy = "NO_HIERARCHY", 
                                                   output_path_shape = "NO_LINES")
  cl_outNALayer = cl_result_object.getOutput(0)
  #Get the names of all the sublayers within the service area layer.
  cl_subLayerNames = arcpy.na.GetNAClassNames(cl_outNALayer)
  #Store the layer names that we will use later
  cl_originsLayerName = cl_subLayerNames["Origins"]
  cl_destinationsLayerName = cl_subLayerNames["Destinations"]
  cl_linesLayerName = cl_subLayerNames["ODLines"]
  # you may have to do this later in the script - but try now....
  cl_ODLinesSubLayer = arcpy.mapping.ListLayers(cl_outNALayer, cl_linesLayerName)[0]
  # define fields and features
  fields = ['Name', 'Total_Length']
  arcpy.MakeFeatureLayer_management (sample_point_feature, "sample_point_feature_layer")
  arcpy.MakeFeatureLayer_management (outCombinedFeature, "destination_points_layer")       
  arcpy.MakeFeatureLayer_management(polygon_feature, "polygon_layer")   
  # initial postgresql connection
  conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
  curs = conn.cursor()  
  
  # define reduced set of destinations and cutoffs (ie. only those with cutoffs defined)
  curs.execute("SELECT DISTINCT(dest_class) FROM dest_type WHERE cutoff_count IS NOT NULL AND count > 0;")
  destination_list = [x[0] for x in list(curs)]
  
  # tally expected parcel-destination class result set  
  curs.execute("SELECT COUNT(*) FROM {sample_point_feature};".format(sample_point_feature = sample_point_feature))
  sample_point_count = list(curs)[0][0]
  completion_goal = sample_point_count * len(destination_list)
  conn.close()

# Custom pandas group by function using numpy 
# Sorts values 'b' as lists grouped by values 'a'  
def list_df_values_by_id(df,a,b):
    df = df[[a,b]]
    keys, values = df.sort_values(a).values.T
    ukeys, index = np.unique(keys,True)
    arrays = np.split(values,index[1:])
    df2 = pandas.DataFrame({a:ukeys,b:[sorted(list(u)) for u in arrays]})
    return df2  
  
# Worker/Child PROCESS
def ODMatrixWorkerFunction(polygon): 
  # Connect to SQL database 
  try:
    conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
    curs = conn.cursor()
    engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db), 
                       use_native_hstore=False)
  except:
    print("SQL connection error")
    print(sys.exc_info()[1])
    return 100
  # make sure Network Analyst licence is 'checked out'
  arcpy.CheckOutExtension('Network')
 
  # Worker Task is polygon-specific by definition/parallel
  # Skip if polygon was finished in previous run
  polygonStartTime = time.time() 
  try:   
    place = "origin selection"  
    # select origin points    
    sql = '''{polygon_id} = {polygon}'''.format(polygon_id = polygon_id, polygon = polygon)
    origin_selection = arcpy.SelectLayerByAttribute_management("sample_point_feature_layer", where_clause = sql)
    origin_point_count = int(arcpy.GetCount_management(origin_selection).getOutput(0))
    # Skip polygons with zero adresses
    if origin_point_count == 0:
        return(2)
    place = 'before polygon selection'
    sql = '''{polygon_id} = {polygon}'''.format(polygon_id=polygon_id,polygon=polygon)
    polygon_selection = arcpy.SelectLayerByAttribute_management("polygon_layer", where_clause = sql)
    place = 'before destination in polygon selection'
    dest_in_polygon = arcpy.SelectLayerByLocation_management("destination_points_layer", 'WITHIN_A_DISTANCE',polygon_selection,3200)
    for dest_class in destination_list:
        destStartTime = time.time()
        result_table = '{distance_schema}."{dest_class}"'.format(distance_schema = distance_schema,
                                                                 dest_class = dest_class)
        # fetch count of successfully processed results for this destination in this polygon
        sql = '''
          SELECT COUNT({result_table}.*)
            FROM {result_table}
        LEFT JOIN {sample_point_feature} p USING ({points_id})
           WHERE p.{polygon_id} = {polygon};
        '''.format(result_table = result_table, 
                   sample_point_feature = sample_point_feature,
                   points_id = points_id,
                   polygon_id = polygon_id,   
                   polygon = polygon,
                   dest_class=dest_class)
        curs.execute(sql)
        already_processed = list(curs)[0][0]
        if already_processed == origin_point_count:
            # update current progress
            place = "update progress (destination already processed)"
            sql = '''
              UPDATE {progress_table} 
              SET processed = processed+{count}
              '''.format(progress_table = progress_table,
                         count = origin_point_count)
            curs.execute(sql)
            conn.commit() 
            return(3)
        remaining_to_process = origin_point_count - already_processed
        sql = '''dest_class = '{}' '''.format(dest_class)
        destination_selection = arcpy.SelectLayerByAttribute_management(dest_in_polygon, where_clause = sql)
        destination_selection_count = int(arcpy.GetCount_management(destination_selection).getOutput(0))
        if destination_selection_count == 0: 
            place = 'zero dest in polygon, so find closest'
            # print(place)
            null_dest_insert = '''
             INSERT INTO {result_table} ({points_id},distances)  
             SELECT p.{points_id},
                    '{curlyo}{curlyc}'::int[]
               FROM {sample_point_feature} p
               LEFT JOIN {result_table} r ON p.{points_id} = r.{points_id}
              WHERE {polygon_id} = {polygon}
                AND r.{points_id} IS NULL
                 ON CONFLICT DO NOTHING;
             '''.format(result_table = result_table,
                        sample_point_feature = sample_point_feature,
                        points_id = points_id,
                        polygon_id = polygon_id,   
                        curlyo = '{',
                        curlyc = '}',                     
                        polygon = polygon)
            # print(null_dest_insert)                    
            curs.execute(null_dest_insert)
            conn.commit()
            # update current progress
            place = "update progress (zero destinations)"
            sql = '''
              UPDATE {progress_table} 
              SET processed = processed+{count}
              '''.format(progress_table = progress_table,
                         count = remaining_to_process)
            curs.execute(sql)
            conn.commit() 
        if already_processed > 0:
            curs.execute('''SELECT p.{points_id} 
                            FROM {sample_point_feature} p 
                            LEFT JOIN {result_table} r ON p.{points_id} = r.{points_id}
                            WHERE {polygon_id} = {polygon}
                              AND r.{points_id} IS NULL;
                         '''.format(result_table = result_table,
                                    sample_point_feature = sample_point_feature,
                                    polygon_id = polygon_id, 
                                    points_id = points_id.lower(), 
                                    polygon = polygon))
            remaining_parcels = [x[0] for x in list(curs)]
            sql = '''
              {polygon_id} = {polygon} AND {points_id} IN ('{parcels}')
              '''.format(polygon = polygon,
                         polygon_id = polygon_id,   
                         points_id = points_id,
                         parcels = "','".join(remaining_parcels))
            origin_subset = arcpy.SelectLayerByAttribute_management("sample_point_feature_layer", 
                                                                    where_clause = sql)
            # OD Matrix Setup      
            arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
                sub_layer                      = originsLayerName, 
                in_table                       = origin_subset, 
                field_mappings                 = "Name {} #".format(points_id), 
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
                field_mappings                 = "Name {} #".format(points_id), 
                search_tolerance               = "{} Meters".format(tolerance), 
                search_criteria                = "{} SHAPE;{} NONE".format(network_edges,network_junctions), 
                append                         = "CLEAR", 
                snap_to_position_along_network = "NO_SNAP", 
                exclude_restricted_elements    = "INCLUDE",
                search_query                   = "{} #;{} #".format(network_edges,network_junctions))
            
        # select destination points 
        arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
            sub_layer                      = destinationsLayerName, 
            in_table                       = destination_selection, 
            field_mappings                 = "Name {} #".format(destination_id), 
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
            sql = '''
             INSERT INTO {result_table} ({points_id},distances)  
             SELECT p.{points_id},
                    '{curlyo}{curlyc}'::int[]
               FROM {sample_point_feature} p
               LEFT JOIN {result_table} r ON p.{points_id} = r.{points_id}
              WHERE {polygon_id} = {polygon}
                AND r.{points_id} IS NULL
                 ON CONFLICT DO NOTHING;
             '''.format(result_table = result_table,
                        sample_point_feature = sample_point_feature,
                        points_id = points_id,
                        polygon_id = polygon_id,   
                        curlyo = '{',
                        curlyc = '}',                     
                        polygon = polygon)
              # print(null_dest_insert)                           
            curs.execute(sql)
            conn.commit()
            place = "update progress (post OD matrix results, no results)"
        else:
            place = 'results were returned, now processing...'
            # Extract lines layer, export to SQL database
            outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)        
            # new pandas approach to od counts
            data = [x for x in outputLines]
            df = pandas.DataFrame(data = data, columns = ['od','distances'])
            df.distances = df.distances.astype('int')
            df[[points_id,'d']] = df['od'].str.split(' - ',expand=True)
            # custom group function
            df = list_df_values_by_id(df,points_id,'distances')
            # df["dest_class"] = dest_class
            # df["polygon"] = polygon
            df = df[[points_id,'distances']]
            # df[points_id] = df[points_id].astype(object)
            # APPEND RESULTS TO EXISTING TABLE
            df.to_sql(result_table,con = engine,index = False, if_exists='append')
            # Where results don't exist for a destination class, ensure a null array is recorded
            sql = '''
             INSERT INTO {result_table} ({points_id}, distances)  
             SELECT p.{points_id},'{curlyo}{curlyc}'::int[] 
               FROM {sample_point_feature} p
               LEFT JOIN {result_table} r ON p.{points_id} = r.{points_id}
             WHERE {polygon_id} = {polygon}
               AND r.{points_id} is NULL
                 ON CONFLICT DO NOTHING;
             '''.format(result_table = result_table,
                        sample_point_feature = sample_point_feature,
                        points_id = points_id.lower(), 
                        polygon_id = polygon_id,   
                        polygon = polygon,
                        curlyo = '{',
                        curlyc = '}')   
            # print(null_dest_insert)                          
            curs.execute(sql)
            conn.commit()       
            place = "update progress (post OD matrix results, successful)"
        # update current progress
        sql = '''
          UPDATE {progress_table} SET processed = processed+{count}
          '''.format(progress_table = progress_table,
                     count = remaining_to_process)
        curs.execute(sql)
        conn.commit()
        place = "check progress"
        sql = '''SELECT processed from {progress_table}'''.format(progress_table = progress_table)
        curs.execute(sql)
        progress = int(list(curs)[0][0])
        place = 'final progress'
        progressor(progress,
                   completion_goal,
                   start,
                   '''{}/{}; last polygon processed: {}, at {}'''.format(progress,
                                                                     completion_goal,
                                                                     polygon,
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
  
  # define reduced set of destinations and cutoffs (ie. only those with cutoffs defined)
  sql = '''SELECT DISTINCT(dest_class) FROM dest_type WHERE cutoff_count IS NOT NULL AND count > 0;'''
  curs.execute(sql)
  destination_list = [x[0] for x in list(curs)]
  
  for dest_class in destination_list:
      result_table = '{distance_schema}."{dest_class}"'.format(distance_schema = distance_schema,
                                                               dest_class = dest_class)
      sql = '''
        DROP TABLE IF EXISTS {result_table};
        CREATE TABLE IF NOT EXISTS {result_table}
        ({points_id} {points_id_type} NOT NULL ,
         distances int[] NOT NULL, 
         PRIMARY KEY({points_id})
         );
         '''.format(result_table=result_table,
                    points_id=points_id,
                    points_id_type=points_id_type)
      curs.execute(sql)
      conn.commit()

  print("Create a table for tracking progress... "), 
  sql = '''
    DROP TABLE IF EXISTS {progress_table};
    CREATE TABLE IF NOT EXISTS {progress_table} AS SELECT 0 AS processed;
    '''.format(progress_table = progress_table,
               result_table = result_table)
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
  # # Iterate process over polygons across nWorkers
  pool.map(ODMatrixWorkerFunction, iteration_list, chunksize=1)
  sql = '''
   SELECT destinations.count * parcels.count, 
          destinations.count, 
          parcels.count, 
          processed.processed
   FROM (SELECT COUNT(DISTINCT(dest_class)) FROM dest_type WHERE cutoff_count IS NOT NULL and count > 0) destinations,
        (SELECT COUNT(*) FROM {sample_point_feature}) parcels,
        (SELECT processed FROM {progress_table}) processed;
  '''.format(sample_point_feature = sample_point_feature,
             progress_table = progress_table)
  curs.execute(sql)
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
