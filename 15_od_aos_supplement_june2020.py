# Script:  12_od_aos_list_analysis.py
# Purpose: Distance to closest POS has been calculated up to 3200 metres to date; 
#          for a range of scenarios (any, 1.5 hectares or larger, with a toilet) 
#          the full distance is to be calculated --- 
#          ie. supplement the current outlying null values (with no recorded destination 
#          within this distance) with the full distance to closest.

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
from _utils import chunks

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

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

## specify "destinations"
os_source = 'open_space_areas'
aos_points   =  'aos_nodes_30m_line'
aos_pointsID =  'aos_entryid'

# SQL Settings
table  = "od_aos"
chunk_size = 600

progress_table = 'processing.od_aos_custom_progress'

# get pid name
pid = multiprocessing.current_process().name
# create initial OD cost matrix layer on worker processors
if pid !='MainProcess':
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
  
  arcpy.MakeFeatureLayer_management(hex_grid, "hex_layer")     
  arcpy.MakeFeatureLayer_management(aos_points, "aos_pointsLayer")   
  arcpy.MakeFeatureLayer_management(origin_points,"origin_pointsLayer")   
        
# Worker/Child PROCESS
def ODMatrixWorkerFunction(hex_id): 
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
  
  try:
    count = 0
    place = 'At the beginning...'
    
    # Get parcel analysis combinations in this hex count
    curs.execute('''SELECT COUNT(*) FROM processing.aos_custom_unbounded WHERE hex_id = {hex_id};'''.format(hex_id=hex_id))
    hex_parcel_analyses_count = int(list(curs)[0][0])
    
    # Get total parcel analyses combinations count
    curs.execute('''SELECT COUNT(*) FROM processing.aos_custom_unbounded;''')
    total_parcel_analyses = int(list(curs)[0][0])
    
    # Get analyses
    sql = '''
    SELECT DISTINCT(analysis) 
      FROM processing.aos_custom_unbounded
     WHERE hex_id = {hex_id};
    '''.format(points_id=points_id, hex_id=hex_id)
    curs.execute(sql)
    analyses = [x[0] for x in list(curs)]
    

    place = 'before analysis loop'
    
    for analysis in analyses:
        # Get points for this analysis-hex combination
        sql = '''
        SELECT {points_id}
          FROM processing.aos_custom_unbounded
         WHERE hex_id = {hex_id}
           AND analysis = $${analysis}$$;
        '''.format(points_id=points_id, hex_id=hex_id,analysis=analysis)
        curs.execute(sql)
        to_do_points = [x[0] for x in list(curs)]
        
        # Get AOS IDs matching criteria
        # note that criteria SQL clause may use PostgreSQl specific techniques, so we'll identify those in database directly
        # then retrieve IDs for matches
        sql = '''
        SELECT DISTINCT(aos_id) aos_id
          FROM open_space_areas
         WHERE {analysis}
         ORDER BY aos_id;
        '''.format(analysis=analysis)
        curs.execute(sql)
        aos_id_selection = ','.join([str(x[0]) for x in list(curs)])
        sql = '''aos_id IN ({aos_id_selection})'''.format(aos_id_selection=aos_id_selection)
        B_selection = arcpy.SelectLayerByAttribute_management("aos_pointsLayer", where_clause = sql)
        B_pointCount = int(arcpy.GetCount_management(B_selection).getOutput(0))
        if B_pointCount == 0:  
            # there are no matching open space of the kind requested in study region; move on
            continue
        
        # Iterate over chunks of points 
        for chunk in chunks(to_do_points,chunk_size):
            sql = '''hex_id = {hex_id} AND {points_id} IN ('{id_list}')'''.format(hex_id = hex_id,
                                                                                  points_id = points_id,
                                                                                  id_list = "','".join(chunk))
            A_selection = arcpy.SelectLayerByAttribute_management("origin_pointsLayer", where_clause = sql)
            # Process OD Matrix Setup
            place = "add unprocessed address points"
            # print(place)
            arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
                sub_layer                      = originsLayerName, 
                in_table                       = A_selection, 
                field_mappings                 = "Name {} #".format(points_id), 
                search_tolerance               = "{} Meters".format(tolerance), 
                search_criteria                = "{} SHAPE;{} NONE".format(network_edges,network_junctions), 
                append                         = "CLEAR", 
                snap_to_position_along_network = "NO_SNAP", 
                exclude_restricted_elements    = "INCLUDE",
                search_query                   = "{} #;{} #".format(network_edges,network_junctions))
            place = "add in parks"
            # print(place
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
                place = 'After solve'
                # Extract lines layer, export to SQL database
                outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)        
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
                    chunkedLines.append('''('{pid}',{aos},{node},{distance})'''.format(pid = pid,
                                                                                       aos = aos,
                                                                                       node = node,
                                                                                       distance  = distance))
                place = "before execute sql, gnaf = {pid}".format(pid=pid)
                # Append results to od_aos table
                data = ','.join(chunkedLines)
                sql = '''
                      INSERT INTO {table} ({points_id}, aos_id, node, distance)  
                      SELECT DISTINCT ON (gnaf_pid, aos_id) gnaf_pid, aos_id, node, distance
                       FROM  
                       (VALUES {data} ) v({points_id}, aos_id, node, distance) 
                      ORDER BY gnaf_pid, aos_id, distance ASC 
                      ON CONFLICT ({points_id}, aos_id) 
                        DO UPDATE
                          SET node = EXCLUDED.node, 
                              distance = EXCLUDED.distance 
                           WHERE EXCLUDED.distance < od_aos.distance;
                      '''.format(points_id = points_id,
                                 table = table,
                                 data   = data) 
                place = "before commit, gnaf = {}".format(pid)
                curs.execute(sql)
                conn.commit()
            if arcpy.Exists(result):  
              arcpy.Delete_management(result)   
    # aggregate processed results as jsonb string
    # Distances to areas of open space matching specific criteria can later be post hoc queried using this
    json_insert = '''
      INSERT INTO {table}_jsonb ({points_id},attributes)  
      SELECT o.{points_id}, 
              jsonb_agg(jsonb_strip_nulls(to_jsonb( 
                  (SELECT d FROM 
                      (SELECT 
                         aos_id,
                         distance
                         ) d)))) AS attributes 
      FROM  od_aos o
      LEFT JOIN processing.aos_custom_unbounded p ON o.{points_id} = p.{points_id}
      WHERE p.hex_id = {hex_id}
        AND p.gnaf_pid IS NOT NULL
      GROUP BY o.{points_id}
      ON CONFLICT ({points_id}) 
      DO UPDATE 
      SET attributes = EXCLUDED.attributes;
      '''.format(points_id = points_id, hex_id = hex_id, table = table)    
    curs.execute(json_insert)
    conn.commit()
    # update current progress
    curs.execute('''UPDATE {progress_table} SET processed = processed+{count}'''.format(progress_table = progress_table,
                                                                                        count = hex_parcel_analyses_count))
    conn.commit()
    curs.execute('''SELECT processed from {progress_table}'''.format(progress_table = progress_table))
    progress = int(list(curs)[0][0])
    status = '''{}/{}; last hex processed: {}, at {}'''.format(progress,
                     total_parcel_analyses,hex_id,time.strftime("%Y%m%d-%H%M%S"))
    progressor(progress,total_parcel_analyses,start,status) 
  except:
    print('''Error: {}
             Place: {}
      '''.format( sys.exc_info(),place))   

  finally:
    arcpy.CheckInExtension('Network')
    conn.close()
  
# MAIN PROCESS
if __name__ == '__main__':
    # simple timer for log file
    start = time.time()
    script = os.path.basename(sys.argv[0])
    task = 'Create OD cost matrix for parcel points to closest POS (any size)'  # Do stuff  
    # Task name is now defined
    print("Commencing task ({}):\n{} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))
    
    # connect to sql
    engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))
    print("Divide work by hexes for multiprocessing, only for parcels not already processed... "),
    # evaluated against od_aos_jsonb as the id field has no duplicates in this table
    sql = '''
    CREATE SCHEMA IF NOT EXISTS processing;
    DROP TABLE IF EXISTS processing.aos_custom_unbounded;
    CREATE TABLE processing.aos_custom_unbounded (
        {points_id} text,
        hex_id integer,
        indicator text,
        analysis text
    );
    '''.format(points_id=points_id)
    engine.execute(sql)
    # Custom OS unbounded analyses
    analyses = {"pos_any":"aos_ha_public > 0",
               "pos_gr_15k_sqm":"aos_ha_public > 1.5",
               "pos_co_location_toilet":"co_location_100m ? 'toilets'"}
    for a in analyses:   
        analysis = analyses[a]
        # Note that we use dollar quoting for text record of analysis as it may contain apostrophes
        # The idea is, 
        #   distinct hexes are divvied out to worker processes
        #   the worker process retrieves the parcels and analyses corresponding to their assigned hex
        #   analyses and parcels are iterated over before updating the final result for that hex
        sql = '''
        INSERT INTO processing.aos_custom_unbounded
        SELECT p.{points_id},hex_id, '{a}'::text AS indicator, $${analysis}$$::text AS analysis
        FROM parcel_dwellings p
        LEFT JOIN 
        (SELECT * FROM
         (SELECT {points_id},
             (obj->>'aos_id')::int AS aos_id, 
             (obj->>'distance')::int AS distance 
            FROM od_aos_jsonb o, 
                jsonb_array_elements(attributes) obj) o
        LEFT JOIN open_space_areas pos ON o.aos_id = pos.aos_id 
        WHERE 
            {analysis}
        ) os_filtered ON p.{points_id} = os_filtered.{points_id}
        WHERE os_filtered.{points_id} IS NULL;
        '''.format(points_id = points_id, a=a, analysis=analysis)
        engine.execute(sql)
    
    sql = '''
    CREATE INDEX IF NOT EXISTS aos_custom_unbounded_idx ON processing.aos_custom_unbounded ({points_id});
    CREATE INDEX IF NOT EXISTS aos_custom_unbounded_hex_idx ON processing.aos_custom_unbounded (hex_id);
    '''.format(points_id=points_id)
    engine.execute(sql)
    sql = '''
    SELECT DISTINCT(hex_id) FROM processing.aos_custom_unbounded ORDER BY hex_id;
    '''
    iteration_list = [x[0] for x in engine.execute(sql)]
    print("Done.")
    
    print("Calculate the sum total of parcels that need to be processed across all analyses..."),
    sql = '''
          DROP TABLE IF EXISTS {progress_table};
          CREATE TABLE {progress_table} AS SELECT 0 processed;
          '''.format(progress_table=progress_table)
    engine.execute(sql)
    print("Done.")
    
    print("Commence multiprocessing...")  
    pool = multiprocessing.Pool(nWorkers)
    pool.map(ODMatrixWorkerFunction, iteration_list, chunksize=1)
    
    print("Post hoc check progress... ")
    sql = '''
    CREATE SCHEMA IF NOT EXISTS processing;
    DROP TABLE IF EXISTS processing.aos_custom_unbounded;
    CREATE TABLE processing.aos_custom_unbounded (
        {points_id} text,
        hex_id integer,
        indicator text,
        analysis text
    );
    '''.format(points_id=points_id)
    engine.execute(sql)
    # Custom OS unbounded analyses
    analyses = {"pos_any":"aos_ha_public > 0",
               "pos_gr_15k_sqm":"aos_ha_public > 1.5",
               "pos_co_location_toilet":"co_location_100m ? 'toilets'"}
    for a in analyses:   
        analysis = analyses[a]
        # Note that we use dollar quoting for text record of analysis as it may contain apostrophes
        # The idea is, 
        #   distinct hexes are divvied out to worker processes
        #   the worker process retrieves the parcels and analyses corresponding to their assigned hex
        #   analyses and parcels are iterated over before updating the final result for that hex
        sql = '''
        INSERT INTO processing.aos_custom_unbounded
        SELECT p.{points_id},hex_id, '{a}'::text AS indicator, $${analysis}$$::text AS analysis
        FROM parcel_dwellings p
        LEFT JOIN 
        (SELECT * FROM
         (SELECT {points_id},
             (obj->>'aos_id')::int AS aos_id, 
             (obj->>'distance')::int AS distance 
            FROM od_aos_jsonb o, 
                jsonb_array_elements(attributes) obj) o
        LEFT JOIN open_space_areas pos ON o.aos_id = pos.aos_id 
        WHERE 
            {analysis}
        ) os_filtered ON p.{points_id} = os_filtered.{points_id}
        WHERE os_filtered.{points_id} IS NULL;
        '''.format(points_id = points_id, a=a, analysis=analysis)
        engine.execute(sql)
    
    sql = '''
    CREATE INDEX IF NOT EXISTS aos_custom_unbounded_idx ON processing.aos_custom_unbounded ({points_id});
    CREATE INDEX IF NOT EXISTS aos_custom_unbounded_hex_idx ON processing.aos_custom_unbounded (hex_id);
    '''.format(points_id=points_id)
    engine.execute(sql)
    sql = '''
    SELECT COUNT(*) FROM processing.aos_custom_unbounded;
    '''
    unresolved_analyses = [x[0] for x in engine.execute(sql)]
    remaining 
    print("There are still {unresolved_analyses} unresolved analyses remaining for this study region.  You can check them by running the following query:\nSELECT * FROM processing.aos_custom_unbounded;\nA good first thing to check if these are otherwise excluded parcels; if so, they have already been excluded for a good reason (e.g. poor network connectivity), and their lack of a result is not suprising and should not be a problem (pending your check that this is so).".format(unresolved_analyses=unresolved_analyses)    
    
    # output to completion log    
    script_running_log(script, task, start, locale)
    engine.dispose()