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
    if len(sys.argv) < 5:
        sys.exit(exit_message)
    elif sys.argv[2] not in ['osm','vicmap']:
        sys.exit(exit_message)
    elif sys.argv[3] not in ['osm','foi','vpa']:
        sys.exit(exit_message)
    elif sys.argv[4] not in ['any','gr1ha','gr1ha_sp']:
        sys.exit(exit_message)

# establish parameters specific to this analysis as per given arguments        
network_abbrev = sys.argv[2]
pos_abbrev = sys.argv[3]
ind_abbrev = sys.argv[4]

pos_suffix = ind_abbrev
if ind_abbrev == 'any':
  pos_suffix == ''

analysis_dict = {"any":"any POS in distance <= 400 m",
                 "gr1ha":"POS >= 1 Ha  in distance <= 400 m",
                 "gr1ha_sp":"POS >= 1 Ha or with a sport in distance <= 400 m"}

this_analysis = analysis_dict[ind_abbrev]      
this_ind = '{network}_{pos}_{ind_abbrev}'.format(network = network,pos = pos,ind_abbrev = ind_abbrev)
   

# ArcGIS environment settings
arcpy.env.workspace = gdb_path  
# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(os.path.join(temp,db)):
    os.makedirs(os.path.join(temp,db))
    
arcpy.env.scratchWorkspace = os.path.join(temp,db)  
arcpy.env.qualifiedFieldNames = False  
arcpy.env.overwriteOutput = True 

# Specify features of interest and key attributes
origin_points   = parcel_dwellings
origin_pointsID = points_id

aos_points = "{pos_abbrev}_nodes_30m_{network}{pos_suffix}".format(pos_abbrev = pos_abbrev,pos_suffix = pos_suffix)   
aos_pointsID =  'aos_entryid'

in_network_dataset = {'osm':'PedestrianRoads\\PedestrianRoads_ND',
                      'vicmap':'pedestrian_vicmap\\pedestrian_vicmap_ND'}[network_abbrev]

# table to contain the results of analyses of this type (combinations of network and pos)                   
aos_threshold = 400
table  = "pos_400m_{ind_abbrev}".format(ind_abbrev = ind_abbrev)
  
                      
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
progress_table = '{table}_progress'.format(table = table)

# get pid name
pid = multiprocessing.current_process().name
# create initial OD cost matrix layer on worker processors
if pid !='MainProcess':
  # Make OD cost matrix layer --- Note: this is only to closest, so limit of 1 destination
  result_object = arcpy.MakeODCostMatrixLayer_na(in_network_dataset = in_network_dataset, 
                                                 out_network_analysis_layer = "ODmatrix", 
                                                 impedance_attribute = "Length", 
                                                 default_cutoff = aos_threshold,
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
  #fields = ['Name', 'Total_Length']  
  fields = ['Name']  
  arcpy.MakeFeatureLayer_management(hex_grid, "hex_layer")     
  arcpy.MakeFeatureLayer_management(aos_points, "aos_pointsLayer")    

  
# Establish preliminary SQL step to filter down Origin-Destination combinations 
# by minimum distance to an entry node
recInsert      = '''
  UPDATE {table} 
     SET {this_ind} = 1
    WHERE {id} IN 
  '''.format(id = origin_pointsID.lower(),
             table = table)          

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
             table = table)  
 
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
    evalulate_intersections = '''
    DROP TABLE IF EXISTS CREATE TABLE aos_temp_hex_{hex};
    CREATE TABLE aos_temp_hex_{hex} AS
    SELECT p.{id}
    FROM parcel_dwellings p
    WHERE EXISTS 
    (SELECT 1 
       FROM parcel_dwellings p, 
            {os_source} o 
      WHERE hex_id = {hex} 
        AND ST_Intersects(p.geom,o.geom)
        AND t.{id} = p.{id});
    SELECT * FROM aos_temp_hex_{hex};
    '''.format(id = points_id.lower(),
               table = table,
               this_ind = this_ind,
               hex = hex[0])
    intersections = pandas.read_sql_query(evalulate_intersections,con=engine)
    intersection_list = incompletions.tolist()
    intersection_list = [p.encode(x) for x in intersection_list]
    if len(intersection_list) > 0:
      evaluate_os_intersection = '''
      UPDATE {table} o SET this_ind = 1 
      WHERE EXISTS (SELECT 1 
                      FROM parcel_dwellings p, 
                           {os_source} o 
                     WHERE hex_id = {hex} 
                       AND ST_Intersects(p.geom,o.geom)
                       AND t.{id} = p.{id});
      '''.format(id = points_id.lower(),
                  hex = hex[0],
                  table = table)
      curs.execute(evaluate_os_intersection)
      conn.commit()
      curs.execute("DROP TABLE aos_temp_hex_{hex}".format(hex=hex[0])
    
    A_selection = arcpy.SelectLayerByAttribute_management("origin_pointsLayer", 
                        where_clause = "hex_id = {hex} AND {id} IN ('{id_list}')".format(hex = hex[0],
                                                                                         id = origin_pointsID,
                                                                                         id_list = to_do_points)) 
    
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
      # '''.format(','.join(['''('{}')'''.format(id) for id in to_do_points]), table = table)
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
        count = 0
        if result[1] == u'true':
          place = 'After solve'
          # Extract lines layer, export to SQL database
          outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)
          curs = conn.cursor()
        
          place = 'before outputLine loop'
          ids_with_pos = [x[0].split('-')[0].encode('utf-8').strip(' ') for x in outputLines]
          count += len(ids_with_pos)
          query = '''UPDATE {table} SET {this_ind} = 1 WHERE {id} IN ({ids})'''.format(table = table,
                                                                                    id = points_id.lower(),
          curs.execute(query)
          conn.commit()
        else:
          ids_without_pos = []
          for outputLine in outputLines:
            count += 1
            od_pair = outputLine[0].split('-')
            id = od_pair[0].encode('utf-8').strip(' ')
            ids_with_pos.append(id)
          
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
  from script_running_log import script_running_log
  task = 'POS in 3200m OD analysis using {} network and {} pos source'.format(network_abbrev,pos_abbrev)
  print("Commencing task ({}):\n{} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))
  print("Locale: {}".format(locale))
  print("Network: {} ({})".format(network_abbrev,in_network_dataset))
  print("POS source: {} ({})".format(pos_abbrev,aos_points))
  print("Output OD matrix: {}".format(table))
    
  # INPUT PARAMETERS
  # connect to sql
  conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
  curs = conn.cursor()
    
    
  print("Creating indicator table {table} for {this_analysis}".format(table = table,
                                                                      this_analysis = this_analysis)),
  ind_table = '''
  CREATE TABLE IF NOT EXISTS {table} AS 
  SELECT {id},
  0::int AS osm_foi_any     ,
  0::int AS osm_osm_any     ,
  0::int AS osm_vpa_any     ,
  0::int AS vicmap_foi_any  ,
  0::int AS vicmap_osm_any  ,
  0::int AS vicmap_vpa_any  ,
  geom
  FROM parcel_dwellings;
  CREATE INDEX IF NOT EXISTS idx_{table} ON {table} ({id});
  '''.format(id = points_id.lower(), table = table)
  curs.execute(ind_table)
  conn.commit()
  print("Done.")
  
  print("Create a table for tracking progress... "), 
  od_aos_progress_table = '''
    DROP TABLE IF EXISTS {table}_progress;
    CREATE TABLE IF NOT EXISTS {table}_progress 
       (processed int);
    '''.format(table = table)
  curs.execute(od_aos_progress_table)
  conn.commit()
  print("Done.")
  
  
  print("Divide work by hexes for multiprocessing, only for parcels not already processed... "),
  antijoin = '''
    SELECT p.hex_id, 
           jsonb_agg(jsonb_strip_nulls(to_jsonb(p.{id}))) AS incomplete
    FROM parcel_dwellings p
    WHERE EXISTS 
    (SELECT 1 FROM {table} s WHERE {this_ind} IS NULL {ind s.{id} = p.{id})
    GROUP BY p.hex_id;
  '''.format(id = points_id.lower(),
             table = table,
             this_ind = this_ind)
  incompletions = pandas.read_sql_query(antijoin,
                                    con=engine)
  to_do_list = incompletions.apply(tuple, axis=1).tolist()
  to_do_list = [[int(x[0]),[p.encode('utf8') for p in x[1]]] for x in to_do_list]
  print("Done.")
  
  print("Calculate the sum total of parcels that need to be processed, and determine the number already processed"),
  to_process = incompletions["incomplete"].str.len().sum()
  processed = total_parcels - to_process
  curs.execute('''INSERT INTO {table}_progress (processed) VALUES ({processed})'''.format(table = table, processed = processed))
  conn.commit()
  print("Done.")
  print("Commence multiprocessing...")  
  progressor(processed,total_parcels,start,"{}/{} at {}".format(processed,total_parcels,time.strftime("%Y%m%d-%H%M%S")))  
  pool = multiprocessing.Pool(nWorkers)
  pool.map(ODMatrixWorkerFunction, to_do_list, chunksize=1)
  
  # output to completion log    
  script_running_log(script, task, start, locale)
  conn.close()