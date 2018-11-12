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
        python script [locale] [network] [pos] [analysis] 
        
        Examples of code to run expressed in this format are:
        python 15b_od_aos_testing_melb_vpa.py melb osm osm    any
        python 15b_od_aos_testing_melb_vpa.py melb osm foi    any
        python 15b_od_aos_testing_melb_vpa.py melb osm vpa    any
        python 15b_od_aos_testing_melb_vpa.py melb vicmap foi any
        python 15b_od_aos_testing_melb_vpa.py melb vicmap vpa any
        python 15b_od_aos_testing_melb_vpa.py melb vicmap osm any

        python 15b_od_aos_testing_melb_vpa.py melb osm osm    gr1ha
        python 15b_od_aos_testing_melb_vpa.py melb osm foi    gr1ha
        python 15b_od_aos_testing_melb_vpa.py melb osm vpa    gr1ha
        python 15b_od_aos_testing_melb_vpa.py melb vicmap foi gr1ha
        python 15b_od_aos_testing_melb_vpa.py melb vicmap vpa gr1ha
        python 15b_od_aos_testing_melb_vpa.py melb vicmap osm gr1ha

        python 15b_od_aos_testing_melb_vpa.py melb osm osm    gr1ha_sp
        python 15b_od_aos_testing_melb_vpa.py melb osm foi    gr1ha_sp
        python 15b_od_aos_testing_melb_vpa.py melb osm vpa    gr1ha_sp
        python 15b_od_aos_testing_melb_vpa.py melb vicmap foi gr1ha_sp
        python 15b_od_aos_testing_melb_vpa.py melb vicmap vpa gr1ha_sp
        python 15b_od_aos_testing_melb_vpa.py melb vicmap osm gr1ha_sp     
        
        Note:
          - it is assumed that earlier scripts have been run and all data is where it is expected to be
          - Specifically, requires scripts 0,1,2,3,4,5,6,11,12,13 and 13b to have been run
          
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
network = sys.argv[2]
pos = sys.argv[3]
ind = sys.argv[4]

pos_suffix = '_{}'.format(ind)
if '_any' in pos_suffix:
  pos_suffix = ''

analysis_dict = {"any":"any POS in distance <= 400 m",
                 "gr1ha":"POS >= 1 Ha  in distance <= 400 m",
                 "gr1ha_sp":"POS >= 1 Ha or with a sport in distance <= 400 m"}

this_analysis = analysis_dict[ind]      
this_ind = '{network}_{pos}'.format(network = network,pos = pos,ind = ind)

os_source = "aos_public_{pos}{pos_suffix}".format(pos = pos,pos_suffix = pos_suffix)

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

aos_points = "{pos}_nodes_30m_{network}{pos_suffix}".format(pos = pos,pos_suffix = pos_suffix, network = network)   
aos_pointsID =  'aos_entryid'
  
in_network_dataset = {'osm':'PedestrianRoads\\PedestrianRoads_ND',
                      'vicmap':'pedestrian_vicmap\\pedestrian_vicmap_ND'}[network]

if __name__ == '__main__':
  if not arcpy.Exists(origin_points): 
    sys.exit("The origin point dataset {} could not be located; exiting.".format(origin_points))
  
  if not arcpy.Exists(aos_points): 
    sys.exit("The AOS nodes dataset {} could not be located; exiting.".format(aos_points))
  
  if not arcpy.Exists(in_network_dataset): 
    sys.exit("The network dataset {} could not be located; exiting.".format(in_network_dataset))
                      
# table to contain the results of analyses of this type (combinations of network and pos)                   
aos_threshold = 400
table  = "pos_400m_{ind}".format(ind = ind)
  
                      
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
  arcpy.MakeFeatureLayer_management(origin_points,"origin_pointsLayer") 

parcel_count = int(arcpy.GetCount_management(origin_points).getOutput(0))  
denominator = parcel_count

# Worker/Child PROCESS
def ODMatrixWorkerFunction(hex): 
  # print("here we go")

  place = "Connect to SQL database "
  # print(place)
  conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
  curs = conn.cursor()
  
  place = "Check out network analyst extension"
  # print(place)
  arcpy.CheckOutExtension('Network')
  
  place = "process start time"
  hexStartTime = time.time()
  count = 0
  
  place = 'Count points as list'
  # print(place)
  to_do_points = hex[1]  
  A_pointCount = len(to_do_points)
  
  place = 'before hex selection'
  # print(place)
  hex_selection = arcpy.SelectLayerByAttribute_management("hex_layer", where_clause = 'OBJECTID = {}'.format(hex[0]))
  
  place = "initialise empty lists of ids with pos"
  # print(place)
  ids_intersecting_pos = []
  ids_near_pos = []
  
  place = "Evaluate intersection of points with AOS"
  # print(place)
  hex_buffer_intersects = '''
  SELECT 1 
   WHERE EXISTS (SELECT 1 
                   FROM  {region}_2018_hex_3000m_diag_3000m_buffer h, 
                          {os_source} p 
                  WHERE h.orig_fid = {hex} 
                    AND ST_Intersects(h.geom,p.geom) 
               GROUP BY h.orig_fid);
  '''.format(region = region,hex = hex[0],os_source = os_source)
  curs.execute(hex_buffer_intersects)
  buffer_intersects = list(curs)
  if len(buffer_intersects) == 0:
    B_pointCount = 0
  else:
    place = "Buffer intersects AOS, so check if hex intersects also"
    # print(place)
    hex_intersects = '''
    SELECT 1 
     WHERE EXISTS (SELECT 1 
                     FROM  {region}_2018_hex_3000m_diag h, 
                           {os_source} p 
                    WHERE h.objectid = {hex} 
                      AND ST_Intersects(h.geom,p.geom) 
                 GROUP BY h.objectid);
    '''.format(region = region,hex = hex[0],os_source = os_source)
    curs.execute(hex_intersects)
    hex_intersects = list(curs)
    if len(hex_intersects) > 0:
      place = "hex intersects, so check if parcels intersect AOS"
      evalulate_intersections = '''
      DROP TABLE IF EXISTS aos_temp_hex_{hex};
      CREATE TABLE aos_temp_hex_{hex} AS
      SELECT p.{id}
      FROM parcel_dwellings p
      WHERE EXISTS 
      (SELECT 1 
         FROM parcel_dwellings t, 
              {os_source} o 
        WHERE t.hex_id = {hex} 
          AND ST_Intersects(t.geom,o.geom)
          AND t.{id} = p.{id})
      GROUP BY p.{id};
      '''.format(id = points_id.lower(),
                 table = table,
                 this_ind = this_ind,
                 hex = hex[0],
                 os_source = os_source)
      place = "evaluate intersections"
      # print(place)
      curs.execute(evalulate_intersections)
      conn.commit()
      curs.execute("SELECT * FROM aos_temp_hex_{hex}".format(hex = hex[0]))
      intersections = list(curs)
      if len(intersections) > 0:
        place = "Points intersects AOS, so update indicator to reflect this"
        # print(place)
        for x in intersections:
          ids_intersecting_pos.append(x[0].encode('utf8'))
        evaluate_os_intersection = '''
        UPDATE {table} t SET {this_ind} = 1 
        WHERE EXISTS (SELECT 1 
                        FROM aos_temp_hex_{hex} p
                       WHERE p.{id} = t.{id});
        '''.format(id = points_id.lower(),
                   this_ind = this_ind,
                   hex = hex[0],
                   table = table)
        curs.execute(evaluate_os_intersection)
        conn.commit()
        # place = "remove intersection points from to do list"
        # # print(place)
        # to_do_points = [x for x in to_do_points if x not in ids_intersecting_pos]
      curs.execute("DROP TABLE aos_temp_hex_{hex}".format(hex=hex[0]))
    
    place = "Select and count parks meeting scenario query"
    # print(place)
    # Note that selection of nodes within 400 meters euclidian distance of the hex 
    # containing the currently selected parcel is sufficient to guarantee the most optimistic
    # route for an edge case given we have accounted for intersection
    # -- ie. a straight line distance of 400m for a parcel on edge of hex
    B_selection = arcpy.SelectLayerByLocation_management('aos_pointsLayer', 'WITHIN_A_DISTANCE', hex_selection, '400 Meters')
    B_pointCount = int(arcpy.GetCount_management(B_selection).getOutput(0))
  place = 'before skip empty B hexes'
  # print(place)    
  if B_pointCount == 0:  
      ids_without_pos = to_do_points
  if B_pointCount > 0:  
      place = "Select origin points"
      # print(place)
      A_selection = arcpy.SelectLayerByAttribute_management("origin_pointsLayer", 
                        where_clause = '''hex_id = {hex} AND {id} NOT IN ('{id_list}')'''.format(hex = hex[0],
                                                                                         id = origin_pointsID,
                                                                                         id_list = "','".join(ids_intersecting_pos))) 
      # Process OD Matrix Setup
      place = "add unprocessed address points"
      # print(place)
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
      place = "add in parks"
      # print(place)
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
      place = 'Solve'
      # print(place)
      result = arcpy.Solve_na(outNALayer, terminate_on_solve_error = "CONTINUE")
  
      if result[1] == u'true':
        place = 'Extract lines layer, export to SQL database'
        # print(place)
        outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)
      
        place = 'before outputLine loop'
        # print(place)
        [ids_near_pos.append(x[0].split('-')[0].encode('utf-8').strip(' ')) for x in outputLines]
        query = '''UPDATE {table} SET {this_ind} = 1 WHERE {id} IN ('{id_list}')'''.format(table = table,
                                                                                     this_ind = this_ind,
                                                                                     id = points_id.lower(),
                                                                                     id_list = "','".join(ids_near_pos))
        curs.execute(query)
        conn.commit()
        place = "identify and update ids without pos"
        # print(place)
        ids_with_pos = ids_intersecting_pos+ids_near_pos
        ids_without_pos = list(set(to_do_points) - set(ids_with_pos))
      else:
        ids_without_pos = list(set(to_do_points) - set(ids_intersecting_pos))
      
      place = "delete result if exists"
      # print(place)
      if arcpy.Exists(result):  
        arcpy.Delete_management(result)   
  place = "Update ind for points without pos"
  # print(place)
  query = '''UPDATE {table} SET {this_ind} = 0 WHERE {id} IN ('{id_list}')'''.format(table = table,
                                                                               this_ind = this_ind,
                                                                               id = points_id.lower(),
                                                                               id_list = "','".join(ids_without_pos))
  curs.execute(query)
  conn.commit()    
  
  curs.execute('''UPDATE {progress_table} SET processed = processed+{count}'''.format(progress_table = progress_table, count = A_pointCount))
  conn.commit()
  curs.execute('''SELECT processed from {progress_table}'''.format(progress_table = progress_table))
  progress = int(list(curs)[0][0])
  ##except:
  ##  print('''
  ##  Error: {}
  ##  Place: {}
  ##  '''.format( sys.exc_info(),place))   
  progressor(progress,total_parcels,start,'''{}/{}; last hex processed: {}, at {}'''.format(progress,total_parcels,hex[0],time.strftime("%Y%m%d-%H%M%S")))  
  arcpy.CheckInExtension('Network')
  conn.close()

# MAIN PROCESS
if __name__ == '__main__':
  from script_running_log import script_running_log
  task = 'POS in 3200m OD analysis using {} network and {} pos source'.format(network,pos)
  print("Commencing task ({}):\n{} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))
  print("Locale: {}".format(locale))
  print("Network: {} ({})".format(network,in_network_dataset))
  print("Analysis: '{}'".format(ind))
  print("POS source: {} ({})".format(pos,aos_points))
  print("Output OD matrix: {}".format(table))
    
  # INPUT PARAMETERS
  # connect to sql
  conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
  curs = conn.cursor()
    
    
  print("Creating indicator table '{table}' for {this_analysis}... ".format(table = table,
                                                                      this_analysis = this_analysis)),
  ind_table = '''
  CREATE TABLE IF NOT EXISTS {table} AS 
  SELECT {id},
  NULL::int AS osm_foi   ,
  NULL::int AS osm_osm   ,
  NULL::int AS osm_vpa   ,
  NULL::int AS vicmap_foi,
  NULL::int AS vicmap_osm,
  NULL::int AS vicmap_vpa,
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
    (SELECT 1 FROM {table} s WHERE {this_ind} IS NULL AND s.{id} = p.{id})
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
  start = time.time()
  progressor(processed,total_parcels,start,"{}/{} at {}".format(processed,total_parcels,time.strftime("%Y%m%d-%H%M%S")))  
  nWorkers = 7
  pool = multiprocessing.Pool(nWorkers)
  pool.map(ODMatrixWorkerFunction, to_do_list, chunksize=1)
  
  # output to completion log    
  script_running_log(script, task, start, locale)
  conn.close()