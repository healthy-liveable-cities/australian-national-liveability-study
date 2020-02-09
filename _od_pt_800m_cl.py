# Script:  15_od_distances_800m.py
# Purpose: This script records the distances to all destinations within 800m, and the closest
#          This allows for mode specific and headway consideration up to 800m, and 
#          evaluation of access to closest 'any' stop up to any distance (e.g. 1600m).
# Authors: Carl Higgs
# Date: 2020-02-08

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

# ArcGIS environment settings
arcpy.env.workspace = gdb_path  
# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(os.path.join(temp,db)):
    os.makedirs(os.path.join(temp,db))
    
arcpy.env.scratchWorkspace = os.path.join(temp,db)  
arcpy.env.qualifiedFieldNames = False  
arcpy.env.overwriteOutput = True 

sample_point_feature = parcel_dwellings
hex_feature = hex_grid

## specify "destinations"
if pt_points in ['','NULL','NaN']:
    sys.exit('Public transport for this analysis has not been defined; see the config parameter "gtfs_all_stops_headway"')
pt_id_orig =  'fid'
# Note: arcpy renames this ot OBJECTID, so unfortunately, we must too
pt_id =  'OBJECTID'

pt_fields = [pt_id,'mode','headway']

if not engine.has_table(pt_points):
    print("\nCopy detailed PT data to postgis..."),
    # get bounding box of buffered study region for clipping using ogr2ogr on import
    sql = '''SELECT ST_Extent(geom) FROM buffered_study_region;'''
    urban_region = engine.execute(sql).fetchone()
    urban_region = [float(x) for x in urban_region[0][4:-1].replace(',',' ').split(' ')]
    # import cropped data
    command = (
            ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
            ' PG:"host={host} port=5432 dbname={db}'
            ' user={user} password = {pwd}" '
            ' {gdb} {feature} -clipsrc {bbox}'
            ' -lco geometry_name="geom"'.format(host = db_host,
                                         db = db,
                                         user = db_user,
                                         pwd = db_pwd,
                                         gdb = src_destinations,
                                         feature = pt_points,
                                         bbox =  '{} {} {} {}'.format(*urban_region)) 
            )
    print(command)
    sp.call(command, shell=True)
    print("Done (although, if it didn't work you can use the printed command above to do it manually)")
    # pgsql to gdb
    print("Copy stops to ArcGIS gdb... "),
    engine.execute(grant_query)
    arcpy.env.workspace = db_sde_path
    arcpy.env.overwriteOutput = True 
    arcpy.CopyFeatures_management('public.{pt_points}'.format(pt_points=pt_points), os.path.join(gdb_path,pt_points)) 
    print("Done.")

# SQL Settings
result_table  = 'od_pt_800m_cl'
progress_table = "progress_pt_800m"

# get pid name
pid = multiprocessing.current_process().name

if pid !='MainProcess':
  # Make 800m OD cost matrix layer
  result_object = arcpy.MakeODCostMatrixLayer_na(in_network_dataset = in_network_dataset, 
                                                 out_network_analysis_layer = "ODmatrix", 
                                                 impedance_attribute = "Length", 
                                                 default_cutoff = 800,
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
  arcpy.MakeFeatureLayer_management (pt_points, "selection_destination_points")     
  arcpy.MakeFeatureLayer_management (pt_points, "all_destination_points")     
  arcpy.MakeFeatureLayer_management(hex_feature, "hex_layer")   
  # cl_sql = '''('{points_id}','{curlyo}{distance}{curlyc}'::int[])'''
  sqlChunkify = 500

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
def ot_pt_process(hex): 
  '''
    Iterate over polygons to calculate network distances to public transport stops.
    
    input: hex
    output: Records results to Postgis database in a long form table
            (contra style for most destinations) containing fields
            gnaf_pid fid mode distance headway
            These allow posthoc querying for PT indicators by mode, distance and headway
            Within 800m, and for closest
  '''
  engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db), 
                       use_native_hstore=False)
  # make sure Network Analyst licence is 'checked out'
  arcpy.CheckOutExtension('Network')
 
  hexStartTime = time.time() 
  try:   
    place = "origin selection"  
    # select origin points    
    sql = '''{hex_id} = {hex}'''.format(hex_id = hex_id, hex = hex)
    origin_selection = arcpy.SelectLayerByAttribute_management("sample_point_feature_layer", 
                                                                where_clause = sql)
    origin_point_count = int(arcpy.GetCount_management(origin_selection).getOutput(0))
    # Skip hexs with zero adresses
    if origin_point_count == 0:
        return(2)
    place = 'before hex selection'
    sql = '''OBJECTID = {hex}'''.format(hex=hex)
    hex_selection = arcpy.SelectLayerByAttribute_management("hex_layer", where_clause = sql)
    place = 'before destination in hex selection'
    dest_within_dist_hex = arcpy.SelectLayerByLocation_management("selection_destination_points", 
                                                         'WITHIN_A_DISTANCE',
                                                         hex_selection,
                                                         800)
    # fetch count of successfully processed results for this destination in this hex
    sql = '''
      SELECT COUNT(*)
        FROM {result_table}
    LEFT JOIN {sample_point_feature} p USING ({points_id})
       WHERE p.{hex_id} = {hex};
    '''.format(result_table = result_table, 
               sample_point_feature = sample_point_feature,
               points_id = points_id,
               hex_id = hex_id,   
               hex = hex)
    already_processed = int(engine.execute(sql).fetchone()[0])
    if already_processed < origin_point_count:
        remaining_to_process = origin_point_count - already_processed
        dest_within_dist_hex_count = int(arcpy.GetCount_management(dest_within_dist_hex).getOutput(0))
        if dest_within_dist_hex_count == 0: 
            place = 'zero dest within analysis distance of hex, solve later'
        # Add origins
        if remaining_to_process < origin_point_count:
            sql = '''SELECT p.{points_id} 
                     FROM {sample_point_feature} p 
                     LEFT JOIN {result_table} r ON p.{points_id} = r.{points_id}
                     WHERE {hex_id} = {hex}
                       AND r.{points_id} IS NULL;
                  '''.format(hex_id = hex_id,
                             result_table = result_table,
                             sample_point_feature = sample_point_feature,
                             points_id = points_id.lower(), 
                             hex = hex)
            remaining_points = pandas.read_sql(sql,engine)
            remaining_points = remaining_points[points_id].astype(str).values
            sql = '''
              {hex_id} = {hex} AND {points_id} IN ('{points}')
              '''.format(hex_id = hex_id,
                         hex = hex,
                         points_id = points_id,
                         points = "','".join(remaining_points))
            origin_subset = arcpy.SelectLayerByAttribute_management("sample_point_feature_layer", 
                                                                    where_clause = sql)
            add_locations(outNALayer,originsLayerName,origin_subset,points_id)
        else:
            add_locations(outNALayer,originsLayerName,origin_selection,points_id)    
        # Add destinations
        add_locations(outNALayer,destinationsLayerName,dest_within_dist_hex,pt_id)
        
        # Process: Solve
        result = arcpy.Solve_na(outNALayer, terminate_on_solve_error = "CONTINUE")
        if result[1] == u'false':
            place = 'OD results processed, but no results recorded in 800m; solve later'
        else:
            place = 'results were returned, now processing...'
            # Extract lines layer, export to SQL database
            outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)        
            # new pandas approach to od counts
            data = [x for x in outputLines]
            df = pandas.DataFrame(data = data, columns = ['od','distance'])
            df.distance = df.distance.astype('int')
            df[[points_id,'fid']] = df['od'].str.split(' - ',expand=True)
            df['fid'] = df['fid'].astype(int)
            df = df[['gnaf_pid','fid','distance']].groupby('gnaf_pid').apply(lambda x: x[['fid','distance']].to_json(orient='records'))
            df = df.reset_index()
            df.columns = [points_id,'attributes']
            place = 'df:\r\n{}'.format(df)
            df.to_sql('{}'.format(result_table),con = engine, index = False, if_exists='append')
        # Solve final closest analysis for points with no destination in 800m
        sql = '''SELECT p.{points_id} 
                        FROM {sample_point_feature} p 
                        LEFT JOIN {result_table} r ON p.{points_id} = r.{points_id}
                        WHERE {hex_id} = {hex}
                          AND r.{points_id} IS NULL;
                     '''.format(result_table = result_table,
                                sample_point_feature = sample_point_feature,
                                hex_id = hex_id, 
                                points_id = points_id.lower(), 
                                hex = hex)
        remaining_points = pandas.read_sql(sql,engine)
        remaining_points = remaining_points[points_id].astype(str).values
        if len(remaining_points) > 0:
            sql = '''
              {hex_id} = {hex} AND {points_id} IN ('{points}')
              '''.format(hex = hex,
                         hex_id = hex_id,   
                         points_id = points_id,
                         points = "','".join(remaining_points))
            origin_subset = arcpy.SelectLayerByAttribute_management("sample_point_feature_layer", 
                                                                    where_clause = sql)     
            add_locations(cl_outNALayer,cl_originsLayerName,origin_subset,points_id)       
            add_locations(cl_outNALayer,cl_destinationsLayerName,"all_destination_points",pt_id)
            # Process: Solve
            result = arcpy.Solve_na(cl_outNALayer, terminate_on_solve_error = "CONTINUE")
            if result[1] == u'false':
                alert = (
                         "\tHex {hex:5} No solution for {n} points"
                         ).format(hex = hex,
                                  n = len(remaining_points))
                print(alert)
                place = 'OD results processed, but no results recorded'
                sql = '''
                 INSERT INTO {result_table} ({points_id},attributes)  
                 SELECT p.{points_id},
                        '{curlyo}{curlyc}'::jsonb
                   FROM {sample_point_feature} p
                   LEFT JOIN {result_table} r ON p.{points_id} = r.{points_id}
                  WHERE {hex_id} = {hex}
                    AND r.{points_id} IS NULL
                     ON CONFLICT DO NOTHING;
                 '''.format(result_table = result_table,
                            sample_point_feature = sample_point_feature,
                            points_id = points_id,
                            hex_id = hex_id,   
                            curlyo = '{',
                            curlyc = '}',                     
                            hex = hex)
                  # print(null_dest_insert)                           
                engine.execute(sql)
            else:
                place = 'OD results processed; results to be recorded'
                outputLines = arcpy.da.SearchCursor(cl_ODLinesSubLayer, fields)   
                data = [x for x in outputLines]
                df = pandas.DataFrame(data = data, columns = ['od','distance'])
                df.distance = df.distance.astype('int')
                df[[points_id,'fid']] = df['od'].str.split(' - ',expand=True)
                df['fid'] = df['fid'].astype(int)
                df = df[['gnaf_pid','fid','distance']].groupby('gnaf_pid').apply(lambda x: x[['fid','distance']].to_json(orient='records'))
                df = df.reset_index()
                df.columns = [points_id,'attributes']
                place = 'df:\r\n{}'.format(df)
                df.to_sql('{}'.format(result_table),con = engine, index = False, if_exists='append')     
  except:
      print('''Error: {}\nhex: {}\nDestination: {}\nPlace: {}\nSQL: {}'''.format( sys.exc_info(),hex,'PT',place,sql))  
  finally:
      arcpy.CheckInExtension('Network')
      engine.dispose()
    
# MAIN PROCESS
if __name__ == '__main__':
  task = 'Record distances and PT stop metadata from origins to PT stops within 800m, and closest'
  print("Commencing task ({}): {} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))
  # # initial postgresql connection
  # conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
  # curs = conn.cursor()  
  
  if not engine.has_table(result_table):
      print("Create result table... "),
      sql = '''
        CREATE TABLE IF NOT EXISTS {result_table}
        ({points_id} {points_id_type} NOT NULL ,
         attributes jsonb
         );
         '''.format(result_table=result_table,
                    points_id=points_id,
                    points_id_type=points_id_type,
                    pt_id = pt_id)
      engine.execute(sql)      
      print("\nDone.")
  else: 
    print("Result table already exists.")
  # Select hexes remaining to be processed
  sql = '''SELECT DISTINCT hex FROM hex_parcels; '''
  hexes = pandas.read_sql(sql, engine)
  iteration_list = hexes['hex'].values
  # Parallel processing setting
  pool = multiprocessing.Pool(processes=nWorkers)
  # # Iterate process over hexs across nWorkers
  # pool.map(ODMatrixWorkerFunction, iteration_list, chunksize=1)
  # # The below code implements a progress counter using hex iterations
  r = list(tqdm(pool.imap(ot_pt_process, iteration_list), total=len(iteration_list), unit='hex'))
  print("\nEnsuring all tables are indexed, and contain only unique ids..."),
  sql = '''
    CREATE UNIQUE INDEX IF NOT EXISTS {result_table}_idx ON  {result_table} ({points_id});
    CREATE INDEX IF NOT EXISTS {pt_points}_mode_idx ON  {pt_points} (mode);
    CREATE INDEX IF NOT EXISTS {pt_points}_headway_idx ON  {pt_points} (headway);
    CREATE INDEX IF NOT EXISTS {result_table}_{pt_id} ON {result_table} ((attributes->'{pt_id_orig}'));
    CREATE INDEX IF NOT EXISTS {result_table}_distance ON od_aos_jsonb ((attributes->'distance'));
    '''.format(result_table=result_table,
               pt_points=pt_points,
               points_id=points_id,
               pt_id = pt_id)
  engine.execute(sql)
  print("Done.")   
  print("\nProcessed results summary:")
  sql = '''
     SELECT 
     (SELECT COUNT(*) FROM {result_table}) AS processed,
     (SELECT COUNT(*) FROM {sample_point_feature}) AS all
    '''.format(result_table = result_table,sample_point_feature=sample_point_feature)
  result_summary = pandas.read_sql(sql, engine)
  with pandas.option_context('display.max_rows', None): 
    print(result_summary)
  print((
         "\nPlease consider the above summary carefully. "
         "\n\nIf any of the above destination tables... "
         "\n- have a distinct processed point count of 0: "
         "\n    - it implies that there are no destinations of this type "
         "\n      accessible within this study region."
         "\n\n"
         "\n- have distinct processed point count > 0 and less than the count of origin points:"
         "\n    - it implies that processing is not fully complete; "
         "\n      it is recommended to run this script again."
         "\n\n"
         "\n - have a distinct processed point count equal to the count of origin points: "
         "\n     - it implies that processing has successfully completed for all points."
         "\n\n"
         "\n   - have a distinct processed point count greater than the count of origin points: "
         "\n       - This should not be possible."
         ))
  # Log completion   
  script_running_log(script, task, start, locale)
  engine.dispose()
