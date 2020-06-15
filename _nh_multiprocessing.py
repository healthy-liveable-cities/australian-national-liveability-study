# Purpose: Generation of service areas (local walkable neighbourhoods) for points (residential addresses)
#          This code is designed to be run by a subprocess python instance
#          orchestrated the main service area creation script in the National Liveability workflow
#          which allocates sets of hexes with approximately equal total number of parcels
#          across the desired number of processors to use for parallel processing
# Carl Higgs, 11 June 2020

import arcpy, arcinfo
import glob
import time
import numpy as np
from sqlalchemy import create_engine
# from tqdm import tqdm
import shutil
from distutils.dir_util import copy_tree

# Import custom variables for National Liveability indicator process
from _project_setup import *

def allocate_processing_bins(table_name,engine, schema=point_schema, id=points_id,sample_points=sample_point_feature):
    '''
    Create allocation of remaining parcels to process grouped by hex in approximately equal size groups.
    
    Given an input table name (the name of the table which is to be processed) generate a corresponding
    processing table in the processing schema with a name based on this input table name.
    
    The function returns the processing table's name, after generating the processing table.
    
    input: table_name string
    output: processing_table string
    '''
    processing_table = f"hex_parcel_{table_name}_remaining"
    sql = f'''
          DROP TABLE IF EXISTS {processing_schema}.{processing_table};
          CREATE TABLE {processing_schema}.{processing_table} AS
          WITH remaining_hex_parcels AS (
          SELECT hex, 
                  hex_parcels.parcel_count - COALESCE(processed.count,0) AS remaining
           FROM hex_parcels
           LEFT JOIN (SELECT hex_id AS hex, 
                             COUNT(a.*) 
                      FROM parcel_dwellings a 
                      LEFT JOIN {schema}.{table_name} b 
                      ON a.{id} = b.{id} 
                      WHERE b.{id} IS NOT NULL
                      GROUP BY hex_id) processed USING (hex)
          )
          SELECT hex,
                 remaining,
                 width_bucket(
                              remaining,                                             -- value to group
                              0,                                                     -- minimum bin size
                              (SELECT MAX(remaining) FROM remaining_hex_parcels), 6  -- maximum bin size
                              ) bin
          FROM   remaining_hex_parcels
          WHERE remaining_hex_parcels.remaining > 0
          GROUP BY hex, remaining
          ORDER BY bin, hex;
          '''
    engine.execute(sql)
    return(processing_table)


def create_walkable_neighbourhood_from_bin(locale_pid_tuple):
    '''
    Given a tuple of locale and processing bin, process local walkable neighbourhood service areas
    (sausage buffers) for the points within hexes allocated to that processing bin.
    
    input: locale_pid_tuple tuple
    output: processed integer
    
    '''
    points_processed = 0
    engine = create_engine(f'''postgresql://{db_user}:{db_pwd}@{db_host}/{db}''', use_native_hstore=False) 
    service_areas_string = ', '.join([str(distance) for distance in service_areas])
    tables = [f'nh{distance}m' for distance in service_areas]
    
    locale,pid=locale_pid_tuple
    
    # ArcGIS environment settings
    arcpy.env.workspace = gdb_path  

    # Specify points
    points = sample_point_feature
    # denominator = int(arcpy.GetCount_management(points).getOutput(0))

    # point chunk size (for looping within polygon)
    group_by = 400
     
    # create project specific folder in temp dir for scratch.gdb, if not exists
    if not os.path.exists(temp):
        os.makedirs(temp)
    
    temp_gdb = f"{temp}/{db}_{pid}.gdb"    
    # make sure Network Analyst licence is 'checked out'
    arcpy.CheckOutExtension('Network')
    # ensure previous gdb is deleted if exists
    if os.path.exists(temp_gdb):
        shutil.rmtree('temp_gdb', ignore_errors=True)
    
    # arcpy.Copy_management(gdb_path, temp_gdb)
    copy_tree(gdb_path, temp_gdb)
    # ArcGIS environment settings
    arcpy.env.workspace = temp_gdb  
    arcpy.env.qualifiedFieldNames = False  
    arcpy.env.overwriteOutput = True 

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

    # Select polygons remaining to be processed
    max_distance = service_areas[-1]
    largest_table = f'nh{distance}m'
    processing_table = f'hex_parcel_{largest_table}_remaining'
    sql = f'''SELECT hex FROM {processing_schema}.{processing_table} WHERE bin={pid}; '''
    iteration_list = [x[0] for x in engine.execute(sql)]
    sql = f'''SELECT SUM(remaining) FROM {processing_schema}.{processing_table} WHERE bin={pid}; '''
    bin_remaining = int([x[0] for x in engine.execute(sql)][0])
    
    for hex in iteration_list:
        # list of point IDs to iterate over
        # assumes that incompletion in the largest table of distances reflects total incompletion
        sql = f'''
        SELECT p.{points_id}
        FROM parcel_dwellings p
        WHERE hex_id = {hex}
        AND NOT EXISTS 
        (SELECT 1 FROM ind_point.{largest_table} s WHERE s.{points_id} = p.{points_id});
        '''
        point_id_list = [x[0] for x in  engine.execute(sql)]
        valid_pointCount = len(point_id_list) 
        row_count = 0
        if valid_pointCount == 0:
            continue
        # Prepare to loop over points within polygons
        id_list = "','".join(point_id_list)
        where_clause = f'''"HEX_ID" = {hex} AND "{points_id}" in ('{id_list}')'''
        arcpy.MakeFeatureLayer_management(points, f"selection_{pid}", where_clause = where_clause)
        pointCount = int(arcpy.GetCount_management(f"selection_{pid}").getOutput(0))
        if pointCount == 0:
            # print('No unprocessed parcels within hex {}; Skipping.'.format(hex))
            continue
        # commence iteration
        count = 1
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
                place = "after AddJoin" 
                arcpy.management.AddJoin(polygons_sublayer, "FacilityID", facilities_sublayer, "ObjectID")
                field_names = [f.name for f in arcpy.ListFields(polygons_sublayer)]
                # print(field_names)
                break_name = [x for x in field_names if x.startswith('SAPolygons') and x.endswith('ToBreak')][0]
                facility_name = [x for x in field_names if x.startswith('Facilities') and x.endswith('Name')][0]
                # write output line features within chunk to Postgresql spatial feature
                sql_chunks = {}
                with arcpy.da.SearchCursor(polygons_sublayer,[facility_name,break_name,'Shape@AREA','Shape@WKT']) as cursor:
                  for row in cursor:
                    id =  row[0]
                    analysis = int(row[1])
                    if analysis not in service_areas:
                        # print(f'''
                               # Error with row: {row} 
                               # (service area '{analysis}' is incorrect, should be in list {service_areas}
                               # Field names context: {field_names}
                               # ''')
                        # This situation arises when network is smaller than the minimum valid network area size 16.5 Ha
                        # for a 1600m network.  That is, the parcel lacks valid connection with the network and must be 
                        # excluded.  Its ID will be inserted into the service area table as a null
                        [engine.execute(f'''INSERT INTO {point_schema}.nh{x}m ({points_id}) VALUES ('{id}') ON CONFLICT ({points_id}) DO NOTHING''') for x in service_areas]
                        continue
                    analysis_table = f'nh{analysis}m'
                    area = int(row[2])
                    wkt = row[3].replace(' NAN','').replace(' M ','')
                    row_sql = f'''('{id}',{area},ST_GeometryFromText('{wkt}',{srid}))'''
                    if analysis_table not in sql_chunks.keys():
                        sql_chunks[analysis_table]=[]
                    sql_chunks[analysis_table].append(row_sql)
                    row_count+=1  
                for analysis_table in sql_chunks:
                    sql = ','.join(sql_chunks[analysis_table])
                    engine.execute(f'''INSERT INTO {point_schema}.{analysis_table} ({points_id},area_sqm, geom) VALUES {sql} ON CONFLICT ({points_id}) DO NOTHING''')
                place = "after SearchCursor"      
                current_floor = (group_by * count)
                count += 1   
                place = "after increment floor and count"  
            except:
                error = sys.exc_info()
                # print(sql)
                sys.exit(f'''HEY, IT'S AN ERROR: {error}
                          ERROR CONTEXT: hex: {hex} current_floor: {current_floor} current_max: {current_max} row_count: {row_count}
                          PLACE: {place}''')
                # raise
            finally:
                # clean up  
                arcpy.Delete_management("tempLayer_{}".format(pid))
        arcpy.Delete_management("selection_{}".format(pid))
    
    arcpy.CheckInExtension('Network')
    engine.dispose()
    # ensure previous gdb is deleted if exists
    if os.path.exists(temp_gdb):
        shutil.rmtree('temp_gdb', ignore_errors=True)
    print(f"\nProcessor {pid} complete\n")
    return(bin_remaining)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('This process is designed to be fed a study region locale and bin allocation number; these arguments do not appear to have been received, so exiting')
    if len(sys.argv) < 3:
        sys.exit('This process is designed to be fed a bin allocation number; this argument does not appear to have been received, so exiting')

    code = sys.argv[0]
    locale = sys.argv[1]    
    pid = sys.argv[2]    
    create_walkable_neighbourhood_from_bin((locale,pid))
    print(f"Received argument to process bin {pid} of results for locale {locale} using the code {code}; however, for the moment this script is designed to be run as part of a multiprocessing routine (e.g. using the concurrent.futures ProcessPoolExecutor function).  It could however be refactored to accept an initial argument indicating the desired function to be run, and then trigger this using the remaining supplied arguments for locale and bin, assuming that processing bins have been established or some fall back allocation of tasks coded in the event that it hasn't been established or the argument not supplied.")