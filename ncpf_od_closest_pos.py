# Script:  ncpf_od_closest_pos.py
# Purpose: This script finds for each A point the closest B point along a network.
#              - it uses parallel processing
#              - it outputs to an sql database 
# Authors: Carl Higgs
#
# Note: Following processing, I would recommend you check out the log_od_distances table 
# in postgresql and consider the entries with 'no solution' - are these reasonable?
# For example - in psql run query 
# SELECT * FROM log_od_distances WHERE status = 'no solution' ORDER BY random() limit 20;
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
destination_pointsID = 'aos_entryid'

# Get

# SQL Settings
## Note - this used to be 'dist_cl_od_parcel_dest' --- simplified to 'od_closest'
od_distances  = "od_closest_pos"
log_table    = "log_od_distances_pos"
maximum_analysis_distance = 400

queryPartA = "INSERT INTO {} VALUES ".format(od_distances)
hexStart = 0
sqlChunkify = 500
  
# initiate postgresql connection
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

destination_list = [['aos_nodes_30m_pos_any','any',400,0],['aos_nodes_30m_pos_large','large',400,1.5]]

# define completion goal (ie. parcel count)
curs.execute("SELECT sum(parcel_count) FROM hex_parcels;")
total_parcels = int(list(curs)[0][0])
progress_table = 'od_aos_progress_ncpf'

# get pid name
pid = multiprocessing.current_process().name

# Define query to create table
createTable     = '''
  DROP TABLE IF EXISTS {0};
  CREATE TABLE IF NOT EXISTS {0}
  ({1} varchar NOT NULL ,
   dest_class varchar NOT NULL ,
   dest_name varchar NOT NULL ,
   oid bigint NOT NULL ,
   distance integer NOT NULL, 
   threshold  int,
   ind_hard   int,
   PRIMARY KEY({1},dest_class)
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

# Worker/Child PROCESS
def ODMatrixWorkerFunction(hex): 
  try:
    # Connect to SQL database 
    conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
    curs = conn.cursor()  
    # select origin points 
    arcpy.MakeFeatureLayer_management (origin_points, "origin_points_layer",where_clause = 'hex_id = {hex_id}'.format(hex_id = hex))
    origin_point_count = int(arcpy.GetCount_management("origin_points_layer").getOutput(0))
    # Skip hexes with zero adresses
    if origin_point_count > 0:    
      # Make OD cost matrix layer
      result_object = arcpy.MakeODCostMatrixLayer_na(in_network_dataset = in_network_dataset, 
                                                     out_network_analysis_layer = "ODmatrix", 
                                                     impedance_attribute = "Length", 
                                                     default_number_destinations_to_find = 1,
                                                     default_cutoff =  maximum_analysis_distance,
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
      
      # for destination_points in remaining_dest_list:
      for destination_points in destination_list:
        # make destination feature layer
        arcpy.MakeFeatureLayer_management (destination_points[0], "destination_points_layer") 
        dest_class = destination_points[1]
        dest_cutoff_threshold = destination_points[2]
        destStartTime = time.time()
        min_size_gr = destination_points[3]
        sql = '''
              SELECT gnaf_pid 
                FROM parcel_dwellings p, 
                     open_space_areas o 
               WHERE hex_id = {hex} 
                 AND o.aos_ha_public > {min_size_gr} 
                 AND ST_DWithin(p.geom,o.geom,{dest_cutoff_threshold});
              '''.format(hex=hex,min_size_gr=min_size_gr,dest_cutoff_threshold=dest_cutoff_threshold)
        curs.execute(sql)
        parcel_near_pos_list = [x[0] for x in list(curs)]
        if len(parcel_near_pos_list) > 0:
          include = '''hex_id = {hex_id} AND {id} IN ('{parcel_near_pos}')'''.format(hex_id = hex, 
                                                                                      id = origin_pointsID,
                                                                                      parcel_near_pos=  "','".join(parcel_near_pos_list))
          # print(include)
          origin_selection = arcpy.SelectLayerByAttribute_management("origin_points_layer", 
                          where_clause = include)
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
              in_table                       = "destination_points_layer", 
              field_mappings                 = "Name {} #".format(destination_pointsID), 
              search_tolerance               = "{} Meters".format(tolerance), 
              search_criteria                = "{} SHAPE;{} NONE".format(network_edges,network_junctions), 
              append                         = "CLEAR", 
              snap_to_position_along_network = "NO_SNAP", 
              exclude_restricted_elements    = "INCLUDE",
              search_query                   = "{} #;{} #".format(network_edges,network_junctions))
          # Process: Solve
          result = arcpy.Solve_na(outNALayer, terminate_on_solve_error = "CONTINUE")
          if result[1] != u'false':
            # Extract lines layer, export to SQL database
            outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)
            count = 0
            chunkedLines = list()
            for outputLine in outputLines:
              count += 1
              origin_id      = outputLine[0].split('-')[0].strip(' ')
              dest_id   = outputLine[0].split('-')[1].split(',')
              dest_name = dest_id[0].strip(' ')
              dest_id   = dest_id[1].strip(' ')
              distance  = int(round(outputLine[1]))
              threshold = float(dest_cutoff_threshold)
              ind_hard  = int(distance < threshold)
              chunkedLines.append('''('{point_id}','{d_class}','{d_name}',{d_id},{distance},{threshold},{ind_h})'''.format(point_id  = origin_id,
                                                                                                                                   d_class = dest_class,
                                                                                                                                   d_name = dest_name,
                                                                                                                                   d_id   = dest_id,
                                                                                                                                   distance  = distance,
                                                                                                                                   threshold = threshold,
                                                                                                                                   ind_h  = ind_hard))
              if(count % sqlChunkify == 0):
                sql = '''
                INSERT INTO {od_distances} AS o VALUES {values} 
                ON CONFLICT ({id},dest_class) 
                DO UPDATE 
                SET dest_name = EXCLUDED.dest_name,
                    oid       = EXCLUDED.oid,
                    distance  = EXCLUDED.distance,
                    ind_hard  = EXCLUDED.ind_hard
                WHERE EXCLUDED.distance < o.distance;
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
              DO UPDATE 
              SET dest_name = EXCLUDED.dest_name,
                  oid       = EXCLUDED.oid,
                  distance  = EXCLUDED.distance,
                  ind_hard  = EXCLUDED.ind_hard
              WHERE EXCLUDED.distance < o.distance;
              '''.format(od_distances=od_distances, 
                          values = ','.join(chunkedLines),
                          id = origin_pointsID)
              curs.execute(sql)
              conn.commit()
  except:
    print(sys.exc_info())

  finally:
    arcpy.CheckInExtension('Network')
    # Report on progress
    curs.execute('''UPDATE {progress_table} SET processed = processed+{count}'''.format(progress_table = progress_table,
                                                                                                 count = origin_point_count))
    conn.commit()
    curs.execute('''SELECT processed from {progress_table}'''.format(progress_table = progress_table))
    progress = int(list(curs)[0][0])
    progressor(progress,total_parcels,start,"{numerator} / {denominator} parcels processed.".format(numerator = progress,denominator = total_parcels))
    # Close SQL connection
    conn.close()

# MAIN PROCESS
if __name__ == '__main__':
  # Task name is now defined
  task = 'Find closest of each pos type entry point to origin'  # Do stuff
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
    
  print("Setup a pool of workers/child processes and split log output..."),
  # Parallel processing setting
  # (now set as parameter in ind_study_region_matrix xlsx file)
  # nWorkers = 7  
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
  conn.close()
  script_running_log(script, task, start, locale)
