# Script:  createSausageBuffer_Loop.py
# Purpose: This script creates service areas for a set of input distances
# Carl Higgs, 2019-20

import arcpy, arcinfo
import glob
import time
import psycopg2 
import numpy as np
from shutil import copytree,rmtree,ignore_patterns
from progressor import progressor
from sqlalchemy import create_engine

from script_running_log import script_running_log

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

# ArcGIS environment settings
arcpy.env.workspace = gdb_path  

# Specify points
points = sample_point_feature
denominator = int(arcpy.GetCount_management(points).getOutput(0))

# temp --- using SSD copies to save write/read time and avoid conflicts
if not os.path.exists(temp):
    os.makedirs(temp)


# initiate postgresql connection
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

engine = create_engine(f'''postgresql://{db_user}:{db_pwd}@{db_host}/{db}''', use_native_hstore=False) 
 
temp_gdb = os.path.join(temp,"scratch_{}".format(db))
# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(temp_gdb):
  os.makedirs(temp_gdb)
      
arcpy.env.scratchWorkspace = temp_gdb 
arcpy.env.qualifiedFieldNames = False  
arcpy.env.overwriteOutput = True 

arcpy.CheckOutExtension('Network')

arcpy.MakeFeatureLayer_management(points, "points")
print("Processing service areas...")
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

        # preparatory set up
        # Process: Make Service Area Layer
        outSAResultObject = arcpy.MakeServiceAreaLayer_na(in_network_dataset = in_network_dataset, 
                                  out_network_analysis_layer = os.path.join(arcpy.env.scratchGDB,"ServiceArea"), 
                                  impedance_attribute = "Length",  
                                  travel_from_to = "TRAVEL_FROM", 
                                  default_break_values = distance, 
                                  line_type="TRUE_LINES",
                                  overlap="OVERLAP", 
                                  polygon_type="NO_POLYS", 
                                  lines_source_fields="NO_LINES_SOURCE_FIELDS", 
                                  hierarchy="NO_HIERARCHY")
                                  
        outNALayer = outSAResultObject.getOutput(0)

        #Get the names of all the sublayers within the service area layer.
        sublayer_names = arcpy.na.GetNAClassNames(outNALayer)
        #Store the layer names that we will use later
        facilitiesLayerName = sublayer_names["Facilities"]
        linesLayerName = sublayer_names["SALines"]
        linesSubLayer = outNALayer.listLayers(linesLayerName)[0]
        facilitiesSubLayer = outNALayer.listLayers(facilitiesLayerName)[0] 
        fcLines  = os.path.join(arcpy.env.scratchGDB,"Lines")

        # Process: Add Locations
        arcpy.AddLocations_na(in_network_analysis_layer = os.path.join(arcpy.env.scratchGDB,"ServiceArea"), 
                    sub_layer                      = facilitiesLayerName, 
                    in_table                       = "points", 
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

        # Dissolve linesLayerName
        # field_names = [f.name for f in arcpy.ListFields(linesSubLayer)]

        arcpy.Dissolve_management(in_features=linesSubLayer, 
                                  out_feature_class=fcLines, 
                                  dissolve_field="FacilityID", 
                                  statistics_fields="", 
                                  multi_part="MULTI_PART", 
                                  unsplit_lines="DISSOLVE_LINES")
        place = "after Dissolve" 

        # Process: Join Field
        arcpy.MakeFeatureLayer_management(fcLines, "tempLayer")  
        place = "after MakeFeatureLayer of TempLayer" 

        arcpy.AddJoin_management(in_layer_or_view = "tempLayer", 
                                 in_field    = "FacilityID", 
                                 join_table  = facilitiesSubLayer,
                                 join_field  = "ObjectId")
        place = "after AddJoin" 

        # write output line features within chunk to Postgresql spatial feature
        # Need to parse WKT output slightly (Postgresql doesn't take this M-values nonsense)
        # with arcpy.da.SearchCursor("tempLayer",['Facilities.Name','Shape@WKT']) as cursor:
        with arcpy.da.SearchCursor("tempLayer",['Facilities3.Name','Shape@WKT']) as cursor:
          for row in cursor:
            id =  row[0]
            wkt = row[1].replace(' NAN','').replace(' M ','')
            # insert into combined nh_areas table?
            # for validation sample - only retain random 20
            sql = f'''
                  INSERT INTO {point_schema}.{table} 
                  SELECT 
                    '{id}',
                    b.area_sqm,
                    b.area_sqm/1000000 AS area_sqkm, 
                    b.area_sqm/10000 AS area_ha,
                    b.geom
                  FROM (SELECT ST_Area(geom) AS area_sqm,
                               geom
                          FROM (SELECT ST_Buffer(ST_SnapToGrid(ST_GeometryFromText('{wkt}', 
                                                                                {srid}),
                                                            {snap_to_grid}),
                                              {line_buffer}) AS geom
                                ) a 
                         ) b ;
            '''
            engine.execute(sql)

        # clean up  
        arcpy.Delete_management("tempLayer")
        arcpy.Delete_management(fcLines)
        # Create sausage buffer spatial index
        engine.execute(f'''CREATE INDEX IF NOT EXISTS {table}_gix ON {point_schema}.{table} USING GIST (geom);''')

        if distance==nh_distance:
            # Create summary table of parcel id and area
            print("    - Creating summary table of points with no 1600m buffer (if any)... "),  
            sql = f'''
            CREATE TABLE IF NOT EXISTS {validation_schema}.no_nh_{distance}m AS 
            SELECT * FROM {sample_point_feature} 
            WHERE {points_id} NOT IN (SELECT {points_id} FROM {point_schema}.{table});
            '''
            engine.execute(sql)
        print("Processed.")








arcpy.Delete_management("points")
arcpy.CheckInExtension('Network')

 
try:
    for gdb in glob.glob("{temp}/scratch_{study_region}_*.gdb"):
      arcpy.Delete_management(gdb)
except: 
    print("FRIENDLY REMINDER!!! Remember to delete temp gdbs to save space!")
    print("(there may be lock files preventing automatic deletion.)")


# Create combined service areas table
areas_sql = []
from_sql = []
for distance in service_areas:
    table = 'nh{}m'.format(distance)
    areas_sql = areas_sql+[f'''{schema}.{table}.area_ha AS {table}_ha''']
    from_sql = from_sql+[f'''LEFT JOIN {schema}.{table} ON p.{points_id} = {schema}.{table}.{points_id}''']

areas_sql=',\n'.join(areas_sql)
from_sql=' \n'.join(from_sql)
sql = f'''
CREATE TABLE IF NOT EXISTS {schema}.service_areas AS
SELECT p.{points_id},
       {areas_sql}
FROM {sample_point_feature} p
{from_sql}
'''
engine.execute(sql)
engine.dispose()

# output to completion log    
script_running_log(script, task, start, locale)
