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
# from progressor import progressor
from tqdm import tqdm

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])

# schema where point indicator output tables will be stored
schema = 'd_3200m_cl'

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

sample_point_feature = sample_point_feature
polygon_feature = polygon_feature

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
  #Store the layer names that we will use later
  subLayerNames = arcpy.na.GetNAClassNames(outNALayer)
  originsLayerName = subLayerNames["Origins"]
  destinationsLayerName = subLayerNames["Destinations"]
  linesLayerName = subLayerNames["ODLines"]
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
  #Store the layer names that we will use later
  cl_subLayerNames = arcpy.na.GetNAClassNames(cl_outNALayer)
  cl_originsLayerName = cl_subLayerNames["Origins"]
  cl_destinationsLayerName = cl_subLayerNames["Destinations"]
  cl_linesLayerName = cl_subLayerNames["ODLines"]
  cl_ODLinesSubLayer = arcpy.mapping.ListLayers(cl_outNALayer, cl_linesLayerName)[0]
  
  # Define fields and features
  fields = ['Name', 'Total_Length']
  arcpy.MakeFeatureLayer_management (sample_point_feature, "sample_point_feature_layer")    
  arcpy.MakeFeatureLayer_management(polygon_feature, "polygon_layer")   
  cl_sql = '''('{points_id}','{curlyo}{distance}{curlyc}'::int[])'''
  sqlChunkify = 500
  
# initial postgresql connection
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

# Compile a list of destinations to be processed
# ie. those which record a positive count in the destination_catalog table for this study region
# and the table for which if existing in the d_3200m_cl schema has a row count of 
# less than the total number of address points used for analysis in this city
# (ie. the count of parcel_dwellings)
sql = '''
SELECT destination
 FROM destination_catalog d
 LEFT JOIN (SELECT table_name, 
                   (xpath('/row/cnt/text()', xml_count))[1]::text::int AS row_count
              FROM (
                SELECT table_name, 
                       table_schema, 
                       query_to_xml(format('SELECT COUNT(*) as cnt 
                                              FROM %I.%I', 
                                                   table_schema, 
                                                   table_name)
                                           , false, true, '') as xml_count
                FROM information_schema.tables
                WHERE table_schema = 'd_3200m_cl' --<< change here for the schema you want
              ) t
           ) completed_tables ON d.destination = completed_tables.table_name
WHERE d.process_od='yes'
  AND d.count > 0
  AND COALESCE(row_count,0) < (SELECT COUNT(*) FROM {sample_point_feature})
ORDER BY destination;
'''.format(sample_point_feature=sample_point_feature)
curs.execute(sql)
destination_list = [x[0] for x in list(curs)]

print("\n")
def list_df_values_by_id(df,a,b):
    """ Custom pandas group by function using numpy 
        Sorts values 'b' as lists grouped by values 'a'  
    """
    df = df[[a,b]]
    keys, values = df.sort_values(a).values.T
    ukeys, index = np.unique(keys,True)
    arrays = np.split(values,index[1:])
    df2 = pandas.DataFrame({a:ukeys,b:[sorted(list(u)) for u in arrays]})
    return df2  

def add_locations(network,sub_layer,in_table,field):
    arcpy.AddLocations_na(in_network_analysis_layer = network, 
        sub_layer                      = sub_layer, 
        in_table                       = in_table, 
        field_mappings                 = "Name {} #".format(field), 
        search_tolerance               = "{} Meters".format(tolerance), 
        search_criteria                = "{} SHAPE;{} NONE".format(network_edges,network_junctions), 
        append                         = "CLEAR", 
        snap_to_position_along_network = "NO_SNAP", 
        exclude_restricted_elements    = "INCLUDE",
        search_query                   = "{} #;{} #".format(network_edges,network_junctions))

# Worker/Child PROCESS
def ODMatrixWorkerFunction(polygon): 
  '''
    Iterate over polygons to processes OD matrices for destinations.
    
    input: polygon
    output: Records results to Postgis database in destination specific tables
            in defined schema (e.g. d_3200m_cl).
            
            Results are recorded for distances to all destinations up to 3200m, or closest,
            (which may be more than 3200m away) in a jsonb list format, indexed by point id.            
            
            Later scripts process the results into indicators.
            
            For example, 
                Continuous indicator:
                  the minimum of each recorded list is the 'closest destination'
                Binary indicator:
                  the smallest value below some threshold distance indicates access within that distance
  '''
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
 
  polygonStartTime = time.time() 
  try:   
    place = "origin selection"  
    # select origin points    
    sql = '''{polygon_id} = {polygon}'''.format(polygon_id = polygon_id, polygon = polygon)
    origin_selection = arcpy.SelectLayerByAttribute_management("sample_point_feature_layer", 
                                                                where_clause = sql)
    origin_point_count = int(arcpy.GetCount_management(origin_selection).getOutput(0))
    # Skip polygons with zero adresses
    if origin_point_count == 0:
        return(2)
    place = 'before polygon selection'
    polygon_selection = arcpy.SelectLayerByAttribute_management("polygon_layer", where_clause = sql)
    place = 'before destination in polygon selection'
    # Loop over destinations
    # for destination in tqdm(destination_list,"polygon: {}".format(polygon)):
    for destination in destination_list:
        arcpy.MakeFeatureLayer_management(destination, "destination_points_layer")   
        dest_in_polygon = arcpy.SelectLayerByLocation_management("destination_points_layer", 
                                                             'WITHIN_A_DISTANCE',
                                                             polygon_selection,
                                                             3200)
        destStartTime = time.time()
        result_table = '{distance_schema}."{destination}"'.format(distance_schema = distance_schema,
                                                                 destination = destination)
        # fetch count of successfully processed results for this destination in this polygon
        sql = '''
          SELECT COUNT(*)
            FROM {result_table}
        LEFT JOIN {sample_point_feature} p USING ({points_id})
           WHERE p.{polygon_id} = {polygon};
        '''.format(result_table = result_table, 
                   sample_point_feature = sample_point_feature,
                   points_id = points_id,
                   polygon_id = polygon_id,   
                   polygon = polygon,
                   destination=destination)
        curs.execute(sql)
        already_processed = list(curs)[0][0]
        if already_processed == origin_point_count:
            continue
        remaining_to_process = origin_point_count - already_processed
        dest_in_polygon_count = int(arcpy.GetCount_management(dest_in_polygon).getOutput(0))
        if dest_in_polygon_count == 0: 
            place = 'zero dest in polygon, solve later'
        # Add origins
        if remaining_to_process > 0:
            curs.execute('''SELECT p.{points_id} 
                            FROM {sample_point_feature} p 
                            LEFT JOIN {result_table} r ON p.{points_id} = r.{points_id}
                            WHERE {polygon_id} = {polygon}
                              AND r.{points_id} IS NULL;
                         '''.format(polygon_id = polygon_id,
                                    result_table = result_table,
                                    sample_point_feature = sample_point_feature,
                                    points_id = points_id.lower(), 
                                    polygon = polygon))
            remaining_points = [str(x[0]) for x in list(curs)]
            if 'int' in points_id_type:
                points = '{}'.format(",".join(remaining_points))
            else:
                points = "'{}'".format("','".join(remaining_points))
            sql = '''
              {polygon_id} = {polygon} AND {points_id} IN ({points})
              '''.format(polygon_id = polygon_id,
                         polygon = polygon,
                         points_id = points_id,
                         points = points)
            origin_subset = arcpy.SelectLayerByAttribute_management("sample_point_feature_layer", 
                                                                    where_clause = sql)
            add_locations(outNALayer,originsLayerName,origin_subset,points_id)
        else:
            add_locations(outNALayer,originsLayerName,origin_selection,points_id)            
        # Add destinations
        add_locations(outNALayer,destinationsLayerName,dest_in_polygon,destination_id)
        
        # Process: Solve
        result = arcpy.Solve_na(outNALayer, terminate_on_solve_error = "CONTINUE")
        if result[1] == u'false':
            place = 'OD results processed, but no results recorded in 3200m; solve later'
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
            # df["destination"] = destination
            # df["polygon"] = polygon
            df = df[[points_id,'distances']]
            # df[points_id] = df[points_id].astype(object)
            # APPEND RESULTS TO EXISTING TABLE
            df = df.drop_duplicates(subset=[points_id])
            place = 'df:\r\n{}'.format(df)
            df.to_sql('{}'.format(destination),con = engine,schema = distance_schema, index = False, if_exists='append')
        # Solve final closest analysis for points with no destination in 3200m
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
        remaining_points = [str(x[0]) for x in list(curs)]
        if 'int' in points_id_type:
            points = '{}'.format(",".join(remaining_points))
        else:
            points = "'{}'".format("','".join(remaining_points))
        if len(remaining_points) > 0:
            sql = '''
              {polygon_id} = {polygon} AND {points_id} IN ({points})
              '''.format(polygon = polygon,
                         polygon_id = polygon_id,   
                         points_id = points_id,
                         points = points)
            origin_subset = arcpy.SelectLayerByAttribute_management("sample_point_feature_layer", 
                                                                    where_clause = sql)     
            destination_selection = arcpy.SelectLayerByAttribute_management("destination_points_layer", "NEW_SELECTION")                                                                        
            add_locations(cl_outNALayer,cl_originsLayerName,origin_subset,points_id)
            add_locations(cl_outNALayer,cl_destinationsLayerName,destination_selection,destination_id)
            # Process: Solve
            result = arcpy.Solve_na(cl_outNALayer, terminate_on_solve_error = "CONTINUE")
            if result[1] == u'false':
                alert = (
                         "\t{destination}\tpolygon {polygon:5} No solution for {n} points"
                         ).format(polygon = polygon,
                                  destination = destination,
                                  n = len(remaining_points))
                print(alert)
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
            else:
                place = 'OD results processed; results to be recorded'
                outputLines = arcpy.da.SearchCursor(cl_ODLinesSubLayer, fields)
                count = 0
                chunkedLines = list()
                for outputLine in outputLines :
                    count += 1
                    origin_id = outputLine[0].split('-')[0].strip(' ')
                    distance  = int(round(outputLine[1]))
                    sql = cl_sql.format(points_id  = origin_id,  
                                        curlyo = '{',
                                        curlyc = '}',      
                                        distance  = distance)
                    chunkedLines.append(sql)
                    if(count % sqlChunkify == 0):
                        sql = '''
                        INSERT INTO {result_table} AS o VALUES {values}
                        '''.format(result_table=result_table, 
                                    values = ','.join(chunkedLines))
                        curs.execute(sql)
                        conn.commit()
                        chunkedLines = list()
                if(count % sqlChunkify != 0):
                    sql = '''
                    INSERT INTO {result_table} AS o VALUES {values} 
                    '''.format(result_table=result_table, 
                                values = ','.join(chunkedLines))
                    curs.execute(sql)
                    conn.commit()            
  except:
      print('''Error: {}\npolygon: {}\nDestination: {}\nPlace: {}\nSQL: {}'''.format( sys.exc_info(),polygon,destination,place,sql))  
  finally:
      arcpy.CheckInExtension('Network')
      conn.close()
   
# MAIN PROCESS
if __name__ == '__main__':
  task = 'Record distances from origins to destinations within 3200m, and closest'
  print("Commencing task ({}): {} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))
  # initial postgresql connection
  conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
  curs = conn.cursor()  
  
  print("Create tables for all destinations listed in destination_catalog table... "),
  sql = '''SELECT DISTINCT(d.destination) destination FROM destination_catalog d ORDER BY destination'''
  curs.execute(sql)
  full_destination_list = [x[0] for x in list(curs)]
  for destination in full_destination_list:
      result_table = '{distance_schema}."{destination}"'.format(distance_schema = distance_schema, destination = destination)
      sql = '''
        CREATE TABLE IF NOT EXISTS {result_table}
        ({points_id} {points_id_type} NOT NULL ,
         distances int[] NOT NULL
         );
        DELETE FROM {result_table} WHERE distances='{curly_braces}';
         '''.format(result_table=result_table,
                    points_id=points_id,
                    points_id_type=points_id_type,
                    curly_braces = '{}')
      curs.execute(sql)
      conn.commit()
      
  print("\nDone.")
  
  print("\nPreviously processed destinations:")
  print([d for d in full_destination_list if d not in destination_list])
  print("\nDestinations to process:")
  print(destination_list)
  
  if len(destination_list) > 0:
      # Parallel processing setting
      pool = multiprocessing.Pool(processes=nWorkers)
      # get list of polygons over which to iterate
      sql = '''
        SELECT DISTINCT {polygon_id} FROM poly_points WHERE count > 0;
          '''.format(polygon_id=polygon_id)
      curs.execute(sql)
      iteration_list = np.asarray([x[0] for x in list(curs)])
      # # Iterate process over polygons across nWorkers
      # pool.map(ODMatrixWorkerFunction, iteration_list, chunksize=1)
      # # The below code implements a progress counter using polygon iterations
      r = list(tqdm(pool.imap(ODMatrixWorkerFunction, iteration_list), total=len(iteration_list), unit='polygon'))
  else:
    print("\nIt seems that results have already been processed for all destinations.")
  print("\nEnsuring all tables are indexed, and contain only unique ids..."),
  for destination in full_destination_list:
      result_table = '{distance_schema}."{destination}"'.format(distance_schema = distance_schema,
                                                               destination = destination)
      sql = '''
        CREATE UNIQUE INDEX IF NOT EXISTS {destination}_idx ON  {result_table} ({id});
         '''.format(result_table=result_table,
                    id=points_id,
                    destination=destination)
      curs.execute(sql)
      conn.commit()
  print("Done.")   
  print("\nProcessed destination summary\n(tables in schema {distance_schema})\n").format(distance_schema = distance_schema)
  engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                    pwd  = db_pwd,
                                                                    host = db_host,
                                                                    db   = db), 
                     use_native_hstore=False)
  sql = '''
     SELECT table_name, 
          (xpath('/row/cnt/text()', xml_count))[1]::text::int AS row_count
     FROM (
       SELECT table_name, 
              table_schema, 
              query_to_xml(format('SELECT COUNT(*) as cnt 
                                     FROM %%I.%%I', 
                                          table_schema, 
                                          table_name)
                                  , false, true, '') as xml_count
       FROM information_schema.tables
       WHERE table_schema = '{distance_schema}' --<< change here for the schema you want
       ORDER BY table_name
     ) t;
    '''.format(distance_schema = distance_schema)
  result_summary = pandas.read_sql(sql, engine)
  with pandas.option_context('display.max_rows', None): 
    print(result_summary)
  print((
         "\nPlease consider the above summary carefully. "
         "\n\nIf any of the above destination tables... "
         "\n- have a row count of 0: "
         "\n    - it implies that there are no destinations of this type "
         "\n      accessible within this study region."
         "\n\n"
         "\n- have row count > 0 and less than the count of origin points:"
         "\n    - it implies that processing is not fully complete; "
         "\n      it is recommended to run this script again."
         "\n\n"
         "\n - have a row count equal to the count of origin points: "
         "\n     - it implies that processing has successfully completed for all points."
         "\n\n"
         "\n   - have a row count greater than the count of origin points: "
         "\n       - it implies that there are duplicate rows recorded; "
         "\n         the attempt to create a unique index will have failed, "
         "\n         and the cause of how duplicate results have been recorded "
         "\n         must be investigated."
         ))
  # Log completion   
  script_running_log(script, task, start, locale)
  conn.close()