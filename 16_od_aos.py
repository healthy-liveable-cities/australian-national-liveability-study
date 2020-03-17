# Purpose: This script records the distances to all destinations within threshold, 
#          and the closest.
#          This allows for mode specific and headway consideration up to threshold, and 
#          evaluation of access to closest 'any' stop up to any distance (e.g. 1600m).
# Authors: Carl Higgs
# Date: 2020-03-17

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
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db), 
                       use_native_hstore=False)
# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
# schema where point indicator output tables will be stored
schema = 'ind_point'

# ArcGIS environment settings
arcpy.env.workspace = gdb_path  
# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(os.path.join(temp,db)):
    os.makedirs(os.path.join(temp,db))
    
arcpy.env.scratchWorkspace = os.path.join(temp,db)  
arcpy.env.qualifiedFieldNames = False  
arcpy.env.overwriteOutput = True 

# SQL Settings

sample_point_feature = sample_point_feature
polygon_feature = polygon_feature

dest_points = 'aos_nodes_30m_line'
threshold = 3200
concept = 'aos'
dest_id =  'aos_id'
# note: aos_id is the identifier of the park, not the specific entry point to 
# that park.  It is assumed that distance to closest point of park and the park itself
# are of interest, rather than the specific entry point used (which if of interest,
# may be gleaned).  By not using the more complex id (e.g. aos_entryid)
# we can save on script complexity (that is a compound string id of 'park,entry point'
# and requires more processing and storage, which are likely redundant)

print("Access analysis for {}\n".format(dest_points))

in_gdb = arcpy.Exists(dest_points)
if in_gdb == False:
    sys.exit('The required points for this analysis do not appear to be located in the destination geodatabase; please ensure it has been correctly specified and imported')

# pt_fields = [dest_id,'mode','headway']

# get pid name
pid = multiprocessing.current_process().name

if pid !='MainProcess':
  # Make OD cost matrix layer
  result_object = arcpy.MakeODCostMatrixLayer_na(in_network_dataset = in_network_dataset, 
                                                 out_network_analysis_layer = "ODmatrix", 
                                                 impedance_attribute = "Length", 
                                                 default_cutoff = threshold,
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
  arcpy.MakeFeatureLayer_management(sample_point_feature, "sample_point_feature_layer")
  arcpy.MakeFeatureLayer_management(dest_points, dest_points)
  arcpy.MakeFeatureLayer_management(polygon_feature, "polygon_layer")   

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
def od_destination_process(polygon_dest_tuple): 
  '''
    Iterate over polygons to calculate network distances to public transport stops.
    
    input: [polygon,points]
    output: Records results to Postgis database in a long form table
            (contra style for most destinations) containing fields
            gnaf_pid fid mode distance headway
            These allow posthoc querying for indicators by locations specific
            attributes, within threshold, and for closest
  '''
  engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db), 
                       use_native_hstore=False)
  # make sure Network Analyst licence is 'checked out'
  arcpy.CheckOutExtension('Network')
  polygonStartTime = time.time() 
  polygon = polygon_dest_tuple[0]
  destination       = polygon_dest_tuple[1]
  result_table = 'od_{}_{}m_cl'.format(concept,threshold)
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
    dest_within_dist_polygon = arcpy.SelectLayerByLocation_management(destination, 
                                                         'WITHIN_A_DISTANCE',
                                                         polygon_selection,
                                                         threshold)
    # fetch count of successfully processed results for this destination in this polygon
    sql = '''
      SELECT COUNT(*)
        FROM {schema}.{result_table}
    LEFT JOIN {sample_point_feature} p USING ({points_id})
       WHERE p.{polygon_id} = {polygon};
    '''.format(result_table = result_table, 
               schema=schema,
               sample_point_feature = sample_point_feature,
               points_id = points_id,
               polygon_id = polygon_id,   
               polygon = polygon)
    already_processed = int(engine.execute(sql).fetchone()[0])
    if already_processed < origin_point_count:
        remaining_to_process = origin_point_count - already_processed
        dest_within_dist_polygon_count = int(arcpy.GetCount_management(dest_within_dist_polygon).getOutput(0))
        if dest_within_dist_polygon_count == 0: 
            place = 'zero dest within analysis distance of polygon, solve later'
        # Add origins
        if remaining_to_process < origin_point_count:
            sql = '''SELECT p.{points_id} 
                     FROM {sample_point_feature} p 
                     LEFT JOIN {schema}.{result_table} r ON p.{points_id} = r.{points_id}
                     WHERE {polygon_id} = {polygon}
                       AND r.{points_id} IS NULL;
                  '''.format(polygon_id = polygon_id,
                             result_table = result_table,
                             schema=schema,
                             sample_point_feature = sample_point_feature,
                             points_id = points_id.lower(), 
                             polygon = polygon)
            remaining_points = pandas.read_sql(sql,engine)
            remaining_points = remaining_points[points_id].astype(str).values
            if 'int' in points_id_type:
                points = '{}'.format(",".join(remaining_points))
            else:
                remaining_points = remaining_points[points_id].astype(str).values
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
        add_locations(outNALayer,destinationsLayerName,dest_within_dist_polygon,dest_id)
        
        # Process: Solve
        result = arcpy.Solve_na(outNALayer, terminate_on_solve_error = "CONTINUE")
        if result[1] == u'false':
            place = 'OD results processed, but no results recorded in threshold; solve later'
        else:
            place = 'results were returned, now processing...'
            # Extract lines layer, export to SQL database
            outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)        
            # new pandas approach to od counts
            data = [x for x in outputLines]
            df = pandas.DataFrame(data = data, columns = ['od','distance'])
            df.distance = df.distance.astype('int')
            df[[points_id,dest_id]] = df['od'].str.split(' - ',expand=True)
            df[dest_id] = df[dest_id].astype(int)
            # In case aos_id is not a unique location,
            # e.g. represents a park with multiple pseudo entry points
            # we take the minimum distance for each point-destination combination
            df = df.groupby([points_id,dest_id])['distance'].min().reset_index()
            df = df[[points_id,dest_id,'distance']].groupby(points_id).apply(lambda x: x[[dest_id,'distance']].to_json(orient='records'))
            df = df.reset_index()
            df.columns = [points_id,'attributes']
            place = 'df:\r\n{}'.format(df)
            df.to_sql('{}'.format(result_table),con = engine, schema=schema, index = False, if_exists='append')
        # Solve final closest analysis for points with no destination in threshold
        sql = '''SELECT p.{points_id} 
                        FROM {sample_point_feature} p 
                        LEFT JOIN {schema}.{result_table} r ON p.{points_id} = r.{points_id}
                        WHERE {polygon_id} = {polygon}
                          AND r.{points_id} IS NULL;
                     '''.format(result_table = result_table,
                                schema=schema,
                                sample_point_feature = sample_point_feature,
                                polygon_id = polygon_id, 
                                points_id = points_id.lower(), 
                                polygon = polygon)
        remaining_points = pandas.read_sql(sql,engine)
        remaining_points = remaining_points[points_id].astype(str).values
        if 'int' in points_id_type:
            points = '{}'.format(",".join(remaining_points))
        else:
            remaining_points = remaining_points[points_id].astype(str).values
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
            add_locations(cl_outNALayer,cl_originsLayerName,origin_subset,points_id)       
            arcpy.SelectLayerByAttribute_management(destination, "CLEAR_SELECTION")
            add_locations(cl_outNALayer,cl_destinationsLayerName,destination,dest_id)
            # Process: Solve
            result = arcpy.Solve_na(cl_outNALayer, terminate_on_solve_error = "CONTINUE")
            if result[1] == u'false':
                alert = (
                         "\tpolygon {polygon:5} No solution for {n} points"
                         ).format(polygon = polygon,
                                  n = len(remaining_points))
                print(alert)
                place = 'OD results processed, but no results recorded'
                sql = '''
                 INSERT INTO {schema}.{result_table} ({points_id},attributes)  
                 SELECT p.{points_id},
                        '{curlyo}{curlyc}'::jsonb
                   FROM {sample_point_feature} p
                   LEFT JOIN {schema}.{result_table} r ON p.{points_id} = r.{points_id}
                  WHERE {polygon_id} = {polygon}
                    AND r.{points_id} IS NULL
                     ON CONFLICT DO NOTHING;
                 '''.format(result_table = result_table,
                            schema=schema,
                            sample_point_feature = sample_point_feature,
                            points_id = points_id,
                            polygon_id = polygon_id,   
                            curlyo = '{',
                            curlyc = '}',                     
                            polygon = polygon)
                  # print(null_dest_insert)                           
                engine.execute(sql)
            else:
                place = 'OD results processed; results to be recorded'
                outputLines = arcpy.da.SearchCursor(cl_ODLinesSubLayer, fields)   
                data = [x for x in outputLines]
                df = pandas.DataFrame(data = data, columns = ['od','distance'])
                df.distance = df.distance.astype('int')
                df[[points_id,dest_id]] = df['od'].str.split(' - ',expand=True)
                df[dest_id] = df[dest_id].astype(int)
                # In case aos_id is not a unique location,
                # e.g. represents a park with multiple pseudo entry points
                # we take the minimum distance for each point-destination combination
                df = df.groupby([points_id,dest_id])['distance'].min().reset_index()
                df = df[[points_id,dest_id,'distance']].groupby(points_id).apply(lambda x: x[[dest_id,'distance']].to_json(orient='records'))
                df = df.reset_index()
                df.columns = [points_id,'attributes']
                place = 'df:\r\n{}'.format(df)
                df.to_sql('{}'.format(result_table),con = engine, schema=schema, index = False, if_exists='append')     
  except:
      print('''Error: {}\npolygon: {}\nDestination: {}\nPlace: {}\nSQL: {}'''.format( sys.exc_info(),polygon,concept,place,sql))  
  finally:
      arcpy.CheckInExtension('Network')
      engine.dispose()
    
# MAIN PROCESS
if __name__ == '__main__':
    task = 'Record distances and {concept} location metadata from origins to {concept} locations within {threshold}m, and closest'.format(concept=concept,threshold=threshold)
    print("Commencing task ({}): {} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))
    # # initial postgresql connection
    # conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
    # curs = conn.cursor()  
    result_table = 'od_{}_{}m_cl'.format(concept,threshold)
    print('\n{}'.format(result_table))
    if not engine.has_table(result_table):
        print("  - create result table '{}'... ".format(result_table)),
        sql = '''
          CREATE TABLE IF NOT EXISTS {schema}.{result_table}
          ({points_id} {points_id_type} NOT NULL ,
           attributes jsonb
           );
           '''.format(result_table=result_table,
                      schema=schema,
                      points_id=points_id,
                      points_id_type=points_id_type,
                      dest_id = dest_id)
        engine.execute(sql)      
        print("\nDone.")
    else: 
      print("  - result table already exists.")
    # Select polygons remaining to be processed
    sql = '''SELECT DISTINCT {polygon_id} FROM poly_points; '''.format(polygon_id=polygon_id)
    polygons = pandas.read_sql(sql, engine)
    iteration_list = [[int(i),dest_points] for i in polygons[polygon_id].values]
    # Parallel processing setting
    pool = multiprocessing.Pool(processes=nWorkers)
    # # Iterate process over polygons across nWorkers
    # # The below code implements a progress counter using polygon iterations
    r = list(tqdm(pool.imap(od_destination_process, iteration_list), total=len(iteration_list), unit='polygon'))
    print("\n  - ensuring all tables are indexed, and contain only unique ids..."),
    sql = '''
      CREATE UNIQUE INDEX IF NOT EXISTS {result_table}_idx ON  {schema}.{result_table} ({points_id});
      CREATE INDEX IF NOT EXISTS {result_table}_{dest_id} ON {schema}.{result_table} ((attributes->'{dest_id}'));
      CREATE INDEX IF NOT EXISTS {result_table}_distance ON {schema}.{result_table} ((attributes->'distance'));
      '''.format(result_table=result_table,
                 points_id=points_id,
                 schema=schema,
                 dest_id=dest_id)
    engine.execute(sql)
    print("Done.")   
    print("  - Processed results summary:")
    sql = '''
       SELECT 
       (SELECT COUNT(*) FROM {schema}.{result_table}) AS processed,
       (SELECT COUNT(*) FROM {sample_point_feature}) AS all
      '''.format(result_table = result_table,schema=schema,sample_point_feature=sample_point_feature)
    result_summary = pandas.read_sql(sql, engine)
    with pandas.option_context('display.max_rows', None): 
      print(result_summary)
    print((
           "\n  Please consider the above summary carefully. "
           "\n  \nIf any of the above destination tables... "
           "\n  - have a distinct processed point count of 0: "
           "\n      - it implies that there are no destinations of this type "
           "\n        accessible within this study region."
           "\n  \n"
           "\n  - have distinct processed point count > 0 and less than the count of origin points:"
           "\n      - it implies that processing is not fully complete; "
           "\n        it is recommended to run this script again."
           "\n  \n"
           "\n   - have a distinct processed point count equal to the count of origin points: "
           "\n       - it implies that processing has successfully completed for all points."
           "\n  \n"
           "\n     - have a distinct processed point count greater than the count of origin points: "
           "\n         - This should not be possible."
           ))
    # Log completion   
    script_running_log(script, task, start, locale)
    engine.dispose()
