# Script:  createSausageBuffer_Loop.py
# Purpose: This script creates service areas for a set of input distances
# Carl Higgs, 2019-20

import arcpy, arcinfo
import glob
import time
import numpy as np
import multiprocessing
from tqdm import tqdm
from shutil import copytree,rmtree,ignore_patterns
from sqlalchemy import create_engine

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
start_string = time.strftime("%Y%m%d-%H%M%S")
script = os.path.basename(sys.argv[0])
service_areas_string = ', '.join([str(x) for x in service_areas])
task = f'create service areas ({service_areas_string}) for locations in {full_locale} based on road network'
print(f"Commencing task: {task} at {start_string}")

schema=point_schema

# get pid name
pid = multiprocessing.current_process().name

# ArcGIS environment settings
arcpy.env.workspace = gdb_path  

# Specify points
points = sample_point_feature
denominator = int(arcpy.GetCount_management(points).getOutput(0))

# point chunk size (for looping within polygon)
group_by = 1000
 
# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(temp):
    os.makedirs(temp)

temp_gdb = f"{temp}/{db}"
# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(temp_gdb):
  os.makedirs(temp_gdb)
      
arcpy.env.scratchWorkspace = temp_gdb 
arcpy.env.qualifiedFieldNames = False  
arcpy.env.overwriteOutput = True 
  
# WORKER PROCESSORS pre-setup
if __name__ != '__main__': 
    # Create database connection
    engine = create_engine(f'''postgresql://{db_user}:{db_pwd}@{db_host}/{db}''', use_native_hstore=False) 
    # preparatory set up
    # Process: Make Service Area Layer
    # Excerpted (add for facilities w/ distance):    default_break_values = "{}".format(distance), 
    outSAResultObject = arcpy.na.MakeServiceAreaAnalysisLayer(
                                  network_data_source = in_network_dataset, 
                                  layer_name = os.path.join(arcpy.env.scratchGDB,"ServiceArea"), 
                                  travel_direction = "FROM_FACILITIES", 
                                  cutoffs = service_areas, 
                                  output_type="POLYGONS", # equivalent of 50m network buffer
                                  geometry_at_overlaps="OVERLAP", 
                                  polygon_detail = "HIGH",
                                  geometry_at_cutoffs="DISKS",
                                  polygon_trim_distance="50 meters"
                                  )                                      
    # get results
    outNALayer = outSAResultObject.getOutput(0)
    #Get the names of all the sublayers within the service area layer.
    sublayer_names = arcpy.na.GetNAClassNames(outNALayer)
    #Store the layer names that we will use later
    facilities_layer_name  = sublayer_names["Facilities"]
    polygons_layer_name = sublayer_names["SAPolygons"]
    # linesLayerName = sublayer_names["SALines"]
    # linesSubLayer = outNALayer.listLayers(linesLayerName)[0]
    facilities_sublayer = outNALayer.listLayers(facilities_layer_name )[0] 
    polygons_sublayer = outNALayer.listLayers(polygons_layer_name )[0] 
    engine.dispose()

def create_service_areas(hex):
    '''
    Function for creating local walkable neighbourhoods, aka service areas, or sausage buffers
    Carl Higgs 2020-06-05
    
    Given an input hex, select points within that hex and process service areas,
    according to global project parameters
    '''
    # Create database connection
    engine = create_engine(f'''postgresql://{db_user}:{db_pwd}@{db_host}/{db}''', use_native_hstore=False) 
    
    arcpy.CheckOutExtension('Network')
    distance = service_areas[-1]
    table = f'nh{distance}m'
    
    # list of point IDs to iterate over
    # assumes that incompletion in the largest table of distances reflects total incompletion
    sql = f'''
    SELECT p.{points_id}
    FROM parcel_dwellings p
    WHERE hex_id = {hex}
    AND NOT EXISTS 
    (SELECT 1 FROM ind_point.{table} s WHERE s.{points_id} = p.{points_id});
    '''
    point_id_list = [x[0] for x in  engine.execute(sql)]
    valid_pointCount = len(point_id_list) 
    if valid_pointCount == 0:
        return(0)  
    # Prepare to loop over points within polygons
    id_list = "','".join(point_id_list)
    where_clause = f'''"HEX_ID" = {hex} AND "{points_id}" in ('{id_list}')'''
    arcpy.MakeFeatureLayer_management(points, f"selection_{pid}", where_clause = where_clause)
    pointCount = int(arcpy.GetCount_management(f"selection_{pid}").getOutput(0))
    if pointCount == 0:
        # print('No unprocessed parcels within hex {}; Skipping.'.format(hex))
        return(0)
    # commence iteration
    row_count = 0
    current_floor = 0
    while (current_floor < valid_pointCount): 
        try:
            # set chunk bounds
            # eg. with group_by size of 200 and initial floor of OID = 0, 
            # initial current_max of 200, next current_floor is 201 with max of 400
            current_max = min(current_floor + group_by,valid_pointCount)
            if current_floor > 0:
                current_floor +=1
            id_list = "','".join(point_id_list[current_floor:current_max+1])
            chunkSQL = f''' "{points_id}" in ('{id_list}')'''
            place = "after defining chunkSQL"
            chunk_group = arcpy.SelectLayerByAttribute_management(f"selection_{pid}", where_clause = chunkSQL)
            place = "after defining chunk_group" 
            # Process: Add Locations
            arcpy.na.AddLocations(in_network_analysis_layer = os.path.join(arcpy.env.scratchGDB,"ServiceArea"), 
                        sub_layer                      = facilities_layer_name , 
                        in_table                       = chunk_group, 
                        field_mappings                 = f"Name {points_id} #", 
                        search_tolerance               = f"{tolerance} Meters", 
                        search_criteria                = f"{network_edges} SHAPE;{network_junctions} NONE", 
                        append                         = "CLEAR", 
                        snap_to_position_along_network = "NO_SNAP", 
                        exclude_restricted_elements    = "INCLUDE",
                        search_query                   = f"{network_edges} #;{network_junctions} #")
            place = "after AddLocations"      
            # Process: Solve
            arcpy.Solve_na(in_network_analysis_layer = os.path.join(arcpy.env.scratchGDB,"ServiceArea"), ignore_invalids = "SKIP",terminate_on_solve_error = "CONTINUE")
            place = "after Solve_na"      
            # field_names = [f.name for f in arcpy.ListFields(linesSubLayer)]
            place = "after AddJoin" 
            arcpy.management.AddJoin(polygons_sublayer, "FacilityID", facilities_sublayer, "ObjectID")
            # write output line features within chunk to Postgresql spatial feature
            with arcpy.da.SearchCursor(polygons_sublayer,['Facilities.Name','SAPolygons.ToBreak','Shape@AREA','Shape@WKT']) as cursor:
              for row in cursor:
                print(row)
                id =  row[0]
                analysis = int(row[1])
                area = int(row[2])
                wkt = row[3].replace(' NAN','').replace(' M ','')
                sql = f'''
                      INSERT INTO {point_schema}.nh{analysis}m
                      SELECT 
                        '{id}',
                        {area} area_sqm,
                        {area}/(1000^2)::numeric area_sqkm,
                        {area}/(100^2)::numeric area_ha,
                        ST_GeometryFromText('{wkt}', {srid}) AS geom
                '''
                engine.execute(sql)
            place = "after SearchCursor"      
            current_floor = (group_by * count)
            count += 1   
            place = "after increment floor and count"  
        except:
           sys.exit('''HEY, IT'S AN ERROR: {}
                    ERROR CONTEXT: hex: {} current_floor: {} current_max: {} row_count: {}
                    PLACE: {}'''.format(sys.exc_info(),hex,current_floor,current_max,row_count,place))
        finally:
           # clean up  
           arcpy.Delete_management("tempLayer_{}".format(pid))
    numerator = [x[0] for x in engine.execute(f"SELECT COUNT(*) FROM ind_point.{table};")][0]
    arcpy.Delete_management("selection_{}".format(pid))
    arcpy.CheckInExtension('Network')
    engine.dispose()
    return(numerator) 

def run():
    task = 'Record distances and PT stop metadata from origins to PT stops within 800m, and closest'
    print("Commencing task ({}): {} at {}".format(db,task,time.strftime("%Y%m%d-%H%M%S")))
    engine = create_engine(f'''postgresql://{db_user}:{db_pwd}@{db_host}/{db}''', use_native_hstore=False) 
    for distance in service_areas:
        print("    - {}m... ".format(distance)),
        table = "nh{}m".format(distance)
        if engine.has_table(table, schema=point_schema):
            print("Aleady exists; skipping.")
        else:
            sql = f'''
              CREATE TABLE IF NOT EXISTS {point_schema}.{table}
                ({points_id} {points_id_type} PRIMARY KEY, 
                 area_sqm   double precision,
                 area_sqkm  double precision,
                 area_ha    double precision,
                 geom geometry);  
              '''
            # create output spatial feature in Postgresql
            engine.execute(sql)

    # Select polygons remaining to be processed
    sql = '''SELECT hex FROM hex_parcels; '''
    iteration_list = [x[0] for x in engine.execute(sql)]
    # pbar = tqdm(total=denominator, unit='polygon')
    # def update(a):
        # pbar.update(a)
    # Parallel processing setting
    pool = multiprocessing.Pool(processes=nWorkers)
    # # Iterate process over polygons across nWorkers
    # # The below code implements a progress counter using polygon iterations
    # pool.apply_async(create_service_areas, iteration_list, callback=update)
    # pool.close()
    r = list(tqdm(pool.imap(create_service_areas, iteration_list), total=len(iteration_list), unit='polygon'))
    # pool.map(create_service_areas, iteration_list, chunksize=1)
    print("\n  - ensuring all tables are indexed, and contain only unique ids..."),

    for distance in service_areas:
        print("    - {}m... ".format(distance)),
        table = "nh{}m".format(distance)
        if engine.has_table(table, schema=point_schema):
            # create index and analyse table
            sql = f'''CREATE INDEX IF NOT EXISTS {table}_gix ON ind_point.{table} USING GIST (geom);ANALYZE {table};'''
            engine.execute(sql)
            euclidean_buffer_area_sqm = int(math.pi*distance**2)
            sql =  '''CREATE TABLE ind_point.pedshed_{distance}m AS
                        SELECT gnaf_pid,
                               {euclidean_buffer_area_sqm} AS euclidean_{distance}m_sqm,
                               s.area_sqm AS nh{distance}m_sqm,
                               s.area_sqm / {euclidean_buffer_area_sqm}.0 AS pedshed_{distance}m
                        FROM ind_point.nh{distance}m s;
                   '''
            engine.execute(sql)

  
    # output to completion log    
    script_running_log(script, task, start, locale)

    # clean up
    engine.dispose()
    try:
        for gdb in glob.glob(os.path.join(temp,"scratch_{}_*.gdb".format(study_region))):
          arcpy.Delete_management(gdb)
    except: 
        print("FRIENDLY REMINDER!!! Remember to delete temp gdbs to save space!")
        print("(there may be lock files preventing automatic deletion.)")