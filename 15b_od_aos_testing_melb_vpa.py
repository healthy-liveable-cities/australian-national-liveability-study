# Script:  15_od_aos_testing_melb_vpa.py
# Purpose: Calcault distance to nearest AOS within 3.2km, 
#          or if none within 3.2km then distance to closest
#
#          This is a test implementation of the script which facilitates
#          comparisons with OSM and VicMap networks for accessing 
#          POS constructed using VPA and FOI data, or open spaces (OS) using OSM
#          In the case of OSM, a post-processing script narrows down to evaluate 
#          access to the subset of AOS that contain OS meeting definition of POS
# Authors: Carl Higgs

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

# first, check if the script was appropriately run
if __name__ == '__main__':
    exit_message = '''
        Please specify a locale, network and public open space data source to run this script. 
        
        The required format is:
        python script locale network pos_source_abbreviation
        
        Examples of code to run are:
        python 15_od_aos_testing_melb_vpa.py melb osm osm
        python 15_od_aos_testing_melb_vpa.py melb osm foi
        python 15_od_aos_testing_melb_vpa.py melb osm vpa
        python 15_od_aos_testing_melb_vpa.py melb vicmap foi
        python 15_od_aos_testing_melb_vpa.py melb vicmap vpa
        python 15_od_aos_testing_melb_vpa.py melb vicmap osm
        
        Note:
          - the osm network with osm pos is not required to be run, 
            since this is dealt with by running script 15 (15_od_aos.py)
          - it is assumed that earlier scripts have been run and all data is where it is expected to be
          
        Good luck!
        '''  
    if len(sys.argv) < 4:
        sys.exit(exit_message)
    elif sys.argv[2] not in ['osm','vicmap']:
        sys.exit(exit_message)
    elif sys.argv[3] not in ['osm','foi','vpa']:
        sys.exit(exit_message)

network_abbrev = sys.argv[2]
pos_abbrev = sys.argv[3]
in_network_dataset = {'osm':'PedestrianRoads\\PedestrianRoads_ND',
            'vicmap':'pedestrian_vicmap\\pedestrian_vicmap_ND'}[network_abbrev]
aos_points = '{pos}_nodes_30m_{network}'.format(network = network_abbrev,
                                                pos = pos_abbrev)
sqlTableName  = "od_aos_{network}_{pos}".format(network = network_abbrev,pos = pos_abbrev)
    
     
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

## specify "destinations" (done above)
aos_pointsID =  'aos_entryid'

hexStart = 0

# SQL Settings
sqlChunkify = 1000
        
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
progress_table = '{table}_progress'.format(table = sqlTableName)

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
  
  arcpy.MakeFeatureLayer_management(hex_grid, "hex_layer")     
  arcpy.MakeFeatureLayer_management(aos_points, "aos_pointsLayer")    

  
# Establish preliminary SQL step to filter down Origin-Destination combinations 
# by minimum distance to an entry node
recInsert      = '''
  INSERT INTO {table} ({id}, aos_id, node, distance)  
  SELECT DISTINCT ON (gnaf_pid, aos_id) gnaf_pid, aos_id, node, min(distance) AS distance
   FROM  
   (VALUES 
  '''.format(id = origin_pointsID.lower(),
             table = sqlTableName)          

# Aggregate the minimum distance OD combinations into a list
# node is retained for verification purposes; 
# ie. we can visually evaluate that the distance to dest checks out  
# Optionally, other attributes could be joined using a 'post' clause with a left join
# and aggregated at this point (see earlier code versions).
# However, it is probably more optimal to keep AOS attributes seperate.
# If making a query re: a specific AOS subset, the AOS IDs for the relevant 
# subset could first be identified; then the OD AOS results could be checked
# to return only those Addresses with subset AOS IDs recorded within the 
# required distance
recUpdate      = '''
  ) v({id}, aos_id, node, distance) 
  GROUP BY {id},aos_id, node
  ON CONFLICT ({id}, aos_id) 
    DO UPDATE
      SET node = EXCLUDED.node, 
          distance = EXCLUDED.distance 
       WHERE {table}.distance > EXCLUDED.distance;
  '''.format(id = origin_pointsID.lower(),
             table = sqlTableName)  
 
parcel_count = int(arcpy.GetCount_management(origin_points).getOutput(0))  
denominator = parcel_count
 
arcpy.MakeFeatureLayer_management(origin_points,"origin_pointsLayer") 
 
## Functions defined for this script    
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]
        
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
    place = 'At the beginning...'
    to_do_points = hex[1]  
    A_pointCount = len(to_do_points)
    # process ids in groups of 500 or fewer
    place = 'before hex selection'
    hex_selection = arcpy.SelectLayerByAttribute_management("hex_layer", where_clause = 'OBJECTID = {}'.format(hex[0]))
    # Evaluate intersection of points with AOS
    evaluate_os_intersection = '''
    INSERT INTO {table} ({id}, aos_id,distance)
    SELECT {id}, 
            aos_id,
            0
    FROM parcel_dwellings p, open_space_areas o
    WHERE hex_id = {hex} 
    AND ST_Intersects(p.geom,o.geom)
    ON CONFLICT ({id}, aos_id) 
      DO UPDATE
         SET distance = 0 
         WHERE {table}.distance > EXCLUDED.distance;;
      '''.format(id = points_id.lower(),
                hex = hex[0],
                table = sqlTableName)
    curs.execute(evaluate_os_intersection)
    conn.commit()
    
    # Select and count parks meeting scenario query
    # Note that selection of nodes within 3200 meters euclidian distance of the hex 
    # containing the currently selected parcel is sufficient to guarantee the most optimistic
    # route for an edge case given we have accounted for intersection
    # -- ie. a straight line distance of 3200m for a parcel on edge of hex
    B_selection = arcpy.SelectLayerByLocation_management('aos_pointsLayer', 'WITHIN_A_DISTANCE', hex_selection, '3200 Meters')
    B_pointCount = int(arcpy.GetCount_management(B_selection).getOutput(0))
    place = 'before skip empty B hexes'
    # Insert nulls then skip if there are no AOS within 3200m of the current hex
    # In theory, this should not occur; these should be filtered out in parcel identification on creation
    # The insertion of nulls assist
    # if B_pointCount == 0:
      # INSERTION OF NULLS SEEMS REDUNDANT
      # update_aos = '''INSERT INTO {table} (gnaf_pid) VALUES {};
      # '''.format(','.join(['''('{}')'''.format(id) for id in to_do_points]), table = sqlTableName)
      # place = "before no B point execute sql, hex = {}".format(hex[0])
      # curs.execute(update_aos)
      # place = "before no B point commit, hex = {}".format(hex[0])
      # conn.commit()
    if B_pointCount > 0:  
      for chunk in chunks(to_do_points,sqlChunkify):
        A_selection = arcpy.SelectLayerByAttribute_management("origin_pointsLayer", 
                        where_clause = "hex_id = {hex} AND {id} IN ('{id_list}')".format(hex = hex[0],
                                                                                         id = origin_pointsID,
                                                                                         id_list = "','".join(chunk)))   
        A_pointCount = int(arcpy.GetCount_management(A_selection).getOutput(0))    
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
        if result[1] == u'true':
          place = 'After solve'
          # Extract lines layer, export to SQL database
          outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)
          curs = conn.cursor()
        
          chunkedLines = list()
          place = 'before outputLine loop'
          for outputLine in outputLines:
            count += 1
            od_pair = outputLine[0].split('-')
            pid = od_pair[0].encode('utf-8').strip(' ')
            aos_pair = od_pair[1].split(',')
            aos = int(aos_pair[0])
            node = int(aos_pair[1])
            distance = int(round(outputLine[1]))
            place = "before chunk append, gnaf = {}".format(pid)
            chunkedLines.append("('{pid}',{aos},{node},{distance})".format(pid = pid,
                                                                     aos = aos,
                                                                     node = node,
                                                                     distance  = distance))
          place = "before execute sql, gnaf = {}".format(pid)
          curs.execute(recInsert + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+recUpdate)
          place = "before commit, gnaf = {}".format(pid)
          conn.commit()
        if arcpy.Exists(result):  
          arcpy.Delete_management(result)   
      
    curs.execute("UPDATE {progress_table} SET processed = processed+{count}".format(progress_table = progress_table,
                                                                                      count = A_pointCount))
    conn.commit()
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
  task = 'POS in 3200m OD analysis using {} network and {} pos source'.format(network_abbrev,pos_abbrev)
  print("Commencing task ({}):\n{} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))
  print("Locale: {}".format(locale))
  print("Network: {} ({})".format(network_abbrev,in_network_dataset))
  print("POS source: {} ({})".format(pos_abbrev,aos_points))
  print("Output OD matrix: {}".format(sqlTableName))
    
  # INPUT PARAMETERS
  # connect to sql
  conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
  curs = conn.cursor()
    
  print("Create Area of Open Space (AOS) within 3200m list table"),
  createTable     = '''
  -- DROP TABLE IF EXISTS {table};
  CREATE TABLE IF NOT EXISTS {table}
  (
  {id} varchar, 
  aos_id int,  
  node int,  
  distance int,
  PRIMARY KEY ({id}, aos_id) 
  );
  '''.format(table = sqlTableName, id = origin_pointsID.lower())
  curs.execute(createTable)
  conn.commit()
  print("Done.")
  ## LATER index on gnaf_id, query
  
  print("Create a table for tracking progress... "), 
  od_aos_progress_table = '''
    DROP TABLE IF EXISTS {table}_progress;
    CREATE TABLE IF NOT EXISTS {table}_progress 
       (processed int);
    '''.format(table = sqlTableName)
  curs.execute(od_aos_progress_table)
  conn.commit()
  print("Done.")
  
  
  print("Divide work by hexes for multiprocessing, only for parcels not already processed... "),
  
  antijoin = '''
    SELECT p.hex_id, 
           jsonb_agg(jsonb_strip_nulls(to_jsonb(p.{id}))) AS incomplete
    FROM parcel_dwellings p
    WHERE NOT EXISTS 
    (SELECT 1 FROM {table} s WHERE s.{id} = p.{id})
    GROUP BY p.hex_id;
  '''.format(id = points_id.lower(),
             table = sqlTableName)
  incompletions = pandas.read_sql_query(antijoin,
                                    con=engine)
  to_do_list = incompletions.apply(tuple, axis=1).tolist()
  to_do_list = [[int(x[0]),[p.encode('utf8') for p in x[1]]] for x in to_do_list]
  print("Done.")
  
  print("Calculate the sum total of parcels that need to be processed, and determine the number already processed"),
  to_process = incompletions["incomplete"].str.len().sum()
  processed = total_parcels - to_process
  curs.execute('''INSERT INTO {table}_progress (processed) VALUES ({processed})'''.format(table = sqlTableName, processed = processed))
  conn.commit()
  print("Done.")
  
  print("Commence multiprocessing...")  
  pool = multiprocessing.Pool(nWorkers)
  pool.map(ODMatrixWorkerFunction, to_do_list, chunksize=1)
  
  print("Create json-ified table, with nested list of AOS within 3200m grouped by address")
  json_table = '''CREATE TABLE {table}_jsonb AS
                  SELECT {id}, 
                          jsonb_agg(jsonb_strip_nulls(to_jsonb( 
                              (SELECT d FROM 
                                  (SELECT 
                                     aos_id,
                                     distance
                                     ) d)))) AS attributes 
                   FROM {table} 
                   GROUP BY {id}'''.format(id = points_id.lower(),
                                           table = sqlTableName)
  curs.execute(json_table)
  conn.commit()
  print("Create indices on attributes")
  curs.execute('''CREATE INDEX idx_{table}_aos_id ON {table}_jsonb ((attributes->'aos_id'));'''.format(table = sqlTableName))
  curs.execute('''CREATE INDEX idx_{table}_distance ON {table}_jsonb ((attributes->'distance'));'''.format(table = sqlTableName))
  conn.commit()
  
  # output to completion log    
  script_running_log(script, task, start, locale)
  conn.close()