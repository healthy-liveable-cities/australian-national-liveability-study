# Script:  createSausageBuffer_Loop.py
# Purpose: This script creates sausage buffers for specified input distance.  
# It:
#  -- iterates over hexes not previously processed
#  -- it loops over points within hexes not previously processed
#  -- outputs the lines from a network service area of specified route length
#  -- the final line feature which is stored in the project postgresql database
#     is then buffered a specified distance
#        -- may be worth considering whether post-processing the st_buffer is worth it
#        -- best approach may be to not permanently st_buffer
#            - could just create a size attribute based on st_buffer
#                 - this is all we require as denominator to density measures
#                 - select by location/distance from existing line is used for catchment
# Input: requires network dataset, parcel points etc -- specified in config.ini
# 
# Note that you can modify the 'nWorkers' variable (line 48 at time of writing)
# to change the number of processors used.  On our computers, 7 should be safe.
# Or, you could use 4 and run two instances of the process (so if one finishes, 
# the other continues to process --- handy over the weekend.
#
# Carl Higgs and Koen Simons, 2016-2018

import arcpy, arcinfo
import glob
import time
import psycopg2 
import numpy as np
from progressor import progressor

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'creates {}{} sausage buffers for locations in {} based on road network {} for those parcels in "no sausage" table for which processing otherwise failed on first pass'.format(distance,units,points,in_network_dataset)

# ArcGIS environment settings
arcpy.env.workspace = gdb_path  
arcpy.env.overwriteOutput = True 

# Specify points
points   = parcel_dwellings
# denominator = int(arcpy.GetCount_management(points).getOutput(0))

# Specify number of processors to use
# e.g. our computers have 8 cores, so to focus on getting one script done 7 cores should be safe
nWorkers = 7

# Output databases
sausage_buffer_table = "sausagebuffer_{}".format(distance)
nh_sausagebuffer_summary = "nh{}m".format(distance)

# point chunk size (for looping within polygon)
group_by = 200

## Log file details (including header row)
log_table = 'log_hex_sausage_buffer'
          
queryInsert      = '''
  INSERT INTO {} VALUES
  '''.format(log_table)

queryUpdate      = '''
  ON CONFLICT ({0}) 
  DO UPDATE SET {1}=EXCLUDED.{1},{2}=EXCLUDED.{2},{3}=EXCLUDED.{3},{4}=EXCLUDED.{4} 
  '''.format('hex','parcel_count','status','moment','mins')  
  
queryInsertSausage = '''
  INSERT INTO {} VALUES
  '''.format(sausage_buffer_table)  
  
# Define log file write method
def writeLog(hex = 0,parcel_count = 0,status = 'NULL',mins = 0, create = log_table):
  try:

    if create == 'create':  
      curs.execute(createTable_log)
      conn.commit()
      
      # print('{:>10} {:>15} {:>20} {:>15} {:>11}'.format('HexID','parcel_count','Status','Time','Minutes'))
    else:
      moment = time.strftime("%Y%m%d-%H%M%S")
      # print to screen regardless
      # print('{:9.0f} {:14.0f}  {:>19s} {:>14s}   {:9.2f}'.format(hex, parcel_count, status, moment, mins))
      
      # write to sql table
      curs.execute("{0} ({1},{2},'{3}','{4}',{5}) {6};".format(queryInsert,hex, parcel_count, status, moment, mins, queryUpdate))
      conn.commit()
      
  except:
    print('''Issue with log file using parameters:
             hex: {}  parcel_count: {}  status: {}   mins:  {}  create:  {}
             '''. format(hex, parcel_count, status, mins, create))

def unique_values(table, field):
  data = arcpy.da.TableToNumPyArray(table, [field])
  return np.unique(data[field])
  

# initiate postgresql connection
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

# preparatory set up
# Process: Make Service Area Layer
outSAResultObject = arcpy.MakeServiceAreaLayer_na(in_network_dataset = in_network_dataset, 
                              out_network_analysis_layer = "ServiceArea", 
                              impedance_attribute = "Length",  
                              travel_from_to = "TRAVEL_FROM", 
                              default_break_values = "{}".format(distance), 
                              line_type="TRUE_LINES",
                              overlap="OVERLAP", 
                              polygon_type="NO_POLYS", 
                              lines_source_fields="NO_LINES_SOURCE_FIELDS", 
                              hierarchy="NO_HIERARCHY")
                              
outNALayer = outSAResultObject.getOutput(0)

#Get the names of all the sublayers within the service area layer.
subLayerNames = arcpy.na.GetNAClassNames(outNALayer)
#Store the layer names that we will use later
facilitiesLayerName = subLayerNames["Facilities"]
linesLayerName = subLayerNames["SALines"]
linesSubLayer = arcpy.mapping.ListLayers(outNALayer,linesLayerName)[0]
facilitiesSubLayer = arcpy.mapping.ListLayers(outNALayer,facilitiesLayerName)[0] 

# fetch list of successfully processed buffers, if any
curs.execute("SELECT COUNT(*) FROM {} ".format(sausage_buffer_table))
processed_point_count = int(list(curs)[0][0])

# fcBufferLines  = "Lines_Buffer_{}".format(hex)
fcLines  = "Lines"
  
# make sure Network Analyst licence is 'checked out'
arcpy.CheckOutExtension('Network')

# list of OIDs to iterate over
curs.execute("SELECT {id} FROM no_sausage;".format(id = points_id.lower())
point_id_list = [x[0] for x in  list(curs)]
valid_pointCount = len(point_id_list)
print("\nOkay, so there are {} points in the no_sausage table; lets have another crack at this...".format(valid_pointCount))
print("\nThis is the valid point list:")
print(point_id_list)
print("\n Now we'll work through these brute force style; it'll be slow but if one fails, we should get the rest. And if one fails we should get a more informative message about why that's happened.")

# oop over points in no sausage table
count = 0
success_count = 0
failure_count = 0
for point in point_id_list:
    place = "select a single no sausage point"
    arcpy.MakeFeatureLayer_management(points, "selection", where_clause = '"gnaf_pid" = {}'.format(point)) 
    place = "add point as a location"
    # Process: Add Locations
    place = "AddLocations" 
    arcpy.AddLocations_na(in_network_analysis_layer = "ServiceArea"), 
                sub_layer                      = facilitiesLayerName, 
                in_table                       = "selection", 
                field_mappings                 = "Name {} #".format(points_id), 
                search_tolerance               = "{} Meters".format(tolerance), 
                search_criteria                = "{} SHAPE;{} NONE".format(network_edges,network_junctions), 
                append                         = "CLEAR", 
                snap_to_position_along_network = "NO_SNAP", 
                exclude_restricted_elements    = "INCLUDE",
                search_query                   = "{} #;{} #".format(network_edges,network_junctions))
     
    
    # Process: Solve
    place = "Solve"
    result = arcpy.Solve_na(in_network_analysis_layer = "ServiceArea", ignore_invalids = "SKIP",terminate_on_solve_error = "CONTINUE")
    if result[1] == u'false':
      failure_count+=1
      count += 1   
    if result[1] != u'false':
      # Dissolve linesLayerName
      place = "Dissolve"
      arcpy.Dissolve_management(in_features=linesSubLayer, 
                                out_feature_class=fcLines, 
                                dissolve_field="FacilityID", 
                                statistics_fields="", 
                                multi_part="MULTI_PART", 
                                unsplit_lines="DISSOLVE_LINES")
      
      # Process: Join Field
      place = "Join"
      arcpy.MakeFeatureLayer_management(fcLines, "tempLayer")  
      place = "after MakeFeatureLayer of TempLayer" 
      
      arcpy.AddJoin_management(in_layer_or_view = "tempLayer", 
                               in_field    = "FacilityID", 
                               join_table  = facilitiesSubLayer,
                               join_field  = "ObjectId")
      
      # write output line features within chunk to Postgresql spatial feature
      # Need to parse WKT output slightly (Postgresql doesn't take this M-values nonsense)
      place = insert to postgis 
      with arcpy.da.SearchCursor("tempLayer",['Facilities.Name','Shape@WKT']) as cursor:
        for row in cursor:
          id =  row[0].encode('utf-8')
          wkt = row[1].encode('utf-8').replace(' NAN','').replace(' M ','')
          curs.execute(queryInsertSausage + "( '{0}',{1},ST_Buffer(ST_SnapToGrid(ST_GeometryFromText('{2}', {3}),{5}),{4}));".format(id,hex,wkt,srid,line_buffer,snap_to_grid))
          place = "after curs.execute insert sausage buffer" 
          conn.commit()
          place = "after conn.commit for insert sausage buffer" 
          row_count+=1  
      count += 1   
      success_count+=1
      progressor(numerator,denominator,start,"{} / {} points processed, with {} successes and {} failures.".format(count,valid_pointCount,success_count,failure_count))

      
# fetch list of successfully processed buffers, if any
curs.execute("SELECT COUNT(*) FROM {} ".format(sausage_buffer_table))
subsequent_point_count = int(list(curs)[0][0])
      
if processed_point_count == subsequent_point_count:
  print(There has been no change in point count. We'll have to look closer or give up.")

if processed_point_count < subsequent_point_count:  
  print("It looks like we have processed an additional {} points; great!".format(subsequent_point_count-processed_point_count))
  # Create sausage buffer spatial index
  print("Creating sausage buffer spatial index... "),
  curs.execute("CREATE INDEX IF NOT EXISTS {0}_gix ON {0} USING GIST (geom);".format(sausage_buffer_table))
  conn.commit()
  print("Done.")
  
  print("Analyze the sausage buffer table to improve performance.")
  curs.execute("ANALYZE {};".format(sausage_buffer_table))
  conn.commit()
  print("Done.")
      
  # Create summary table of parcel id and area
  print("Creating summary table of points with no sausage (are they mostly non-urban?)... "),  
  curs.execute("DROP TABLE IF EXISTS no_sausage; CREATE TABLE no_sausage AS SELECT * FROM parcel_dwellings WHERE {0} NOT IN (SELECT {0} FROM {1});".format(points_id,sausage_buffer_table))
  conn.commit()    
  print("Done.")
  
  # output to completion log    
  script_running_log(script, task, start, locale)
  
  # clean up
  conn.close()
