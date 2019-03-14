# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# update variables to allow reference to real, rather than test, data
db = 'li_albury_wodonga_2018'
gdb_path = 'D:/ntnl_li_2018_template/data/study_region/albury_wodonga/li_albury_wodonga_2018.gdb'
hex_grid = 'sua_2018_hex_3000m_diag' 
# define a test destination
dest_class = ('supermarket',)

# set specific hex details
hex = 265

import arcpy, arcinfo
import os
import time
import multiprocessing
import sys
import psycopg2 
import numpy as np

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
origin_pointsID = points_id

## specify "destination_points" (e.g. destinations)
destination_pointsID = destination_id

# Get a list of feature 
featureClasses = arcpy.ListFeatureClasses()


# SQL Settings
## Note - this used to be 'dist_cl_od_parcel_dest' --- simplified to 'result_table'
result_table = "od_distances_3200m"
progress_table = "{table}_progress".format(table = result_table)

# SQL insert queries
insert1 = '''INSERT INTO {table} ({id}, hex, dest_class, distances) SELECT {id},'''.format(table = result_table,
                                                                                       id = origin_pointsID.lower())
insert2 = ''' AS hex, dest_class, array_agg(distance) AS distances FROM (VALUES '''     
insert3 = ''') v({id}, dest_class, distance) GROUP BY {id}, dest_class '''.format(id = origin_pointsID.lower()) 
insert4 = '''ON CONFLICT DO NOTHING;'''


# Make OD cost matrix layer
result_object = arcpy.MakeODCostMatrixLayer_na(in_network_dataset = in_network_dataset, 
                                               out_network_analysis_layer = "ODmatrix", 
                                               impedance_attribute = "Length", 
                                               default_cutoff = 3200,
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
arcpy.MakeFeatureLayer_management (origin_points, "origin_points_layer")
arcpy.MakeFeatureLayer_management (outCombinedFeature, "destination_points_layer")      

arcpy.MakeFeatureLayer_management(hex_grid, "hex_layer")   

# initial postgresql connection
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

# define reduced set of destinations and cutoffs (ie. only those with cutoffs defined)
curs.execute("SELECT DISTINCT(dest_class) FROM dest_type WHERE cutoff_count IS NOT NULL AND count > 0;")
destination_list = list(curs)

place = "origin selection"  
# select origin points    
origin_selection = arcpy.SelectLayerByAttribute_management("origin_points_layer", where_clause = 'HEX_ID = {}'.format(hex))
origin_point_count = int(arcpy.GetCount_management(origin_selection).getOutput(0))


place = 'before hex selection'
hex_selection = arcpy.SelectLayerByAttribute_management("hex_layer", where_clause = 'OBJECTID = {}'.format(hex))
place = 'before destination in hex selection'
dest_in_hex = arcpy.SelectLayerByLocation_management("destination_points_layer", 'WITHIN_A_DISTANCE',hex_selection,3200)
dest_in_hex_count = int(arcpy.GetCount_management(dest_in_hex).getOutput(0))

# test specific destination, previously defined
dest_count = dest_class[1]
origin_dest_point_count = origin_point_count - dest_count
dest_class = dest_class[0]
destStartTime = time.time()
if dest_count > 0:
  curs.execute('''SELECT {id} 
                  FROM parcel_dwellings p 
                  WHERE hex_id = {hex}
                  AND NOT EXISTS (SELECT 1 FROM {table} o 
                                  WHERE hex = {hex}
                                    AND dest_class = '{dest_class}' 
                                    AND p.{id} = o.{id});
               '''.format(table = result_table,
                          id = origin_pointsID.lower(), 
                          hex = hex,
                          dest_class = dest_class))
  remaining_parcels = [x[0] for x in list(curs)]
  origin_subset = arcpy.SelectLayerByAttribute_management("origin_points_layer", 
                                                          where_clause = "HEX_ID = {hex} AND {id} IN ('{parcels}')".format(hex = hex,
                                                                                                                           id = origin_pointsID.lower(),
                                                                                                                           parcels = "','".join(remaining_parcels)))
  # OD Matrix Setup      
  arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
      sub_layer                      = originsLayerName, 
      in_table                       = origin_subset, 
      field_mappings                 = "Name {} #".format(origin_pointsID), 
      search_tolerance               = "{} Meters".format(tolerance), 
      search_criteria                = "{} SHAPE;{} NONE".format(network_edges,network_junctions), 
      append                         = "CLEAR", 
      snap_to_position_along_network = "NO_SNAP", 
      exclude_restricted_elements    = "INCLUDE",
      search_query                   = "{} #;{} #".format(network_edges,network_junctions))
else:
  # OD Matrix Setup      
  arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
      sub_layer                      = originsLayerName, 
      in_table                       = origin_selection, 
      field_mappings                 = "Name {} #".format(origin_pointsID), 
      search_tolerance               = "{} Meters".format(tolerance), 
      search_criteria                = "{} SHAPE;{} NONE".format(network_edges,network_junctions), 
      append                         = "CLEAR", 
      snap_to_position_along_network = "NO_SNAP", 
      exclude_restricted_elements    = "INCLUDE",
      search_query                   = "{} #;{} #".format(network_edges,network_junctions))

# select destination points 
destination_selection = arcpy.SelectLayerByAttribute_management(dest_in_hex, where_clause = "dest_class = '{}'".format(dest_class))
arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
    sub_layer                      = destinationsLayerName, 
    in_table                       = destination_selection, 
    field_mappings                 = "Name {} #".format(destination_pointsID), 
    search_tolerance               = "{} Meters".format(tolerance), 
    search_criteria                = "{} SHAPE;{} NONE".format(network_edges,network_junctions), 
    append                         = "CLEAR", 
    snap_to_position_along_network = "NO_SNAP", 
    exclude_restricted_elements    = "INCLUDE",
    search_query                   = "{} #;{} #".format(network_edges,network_junctions))

# Process: Solve
result = arcpy.Solve_na(outNALayer, terminate_on_solve_error = "CONTINUE")
place = 'results were returned, now processing...'
# Extract lines layer, export to SQL database
outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)        
chunkedLines = list()
# Result table queries
place = 'before outputLine loop'
for outputLine in outputLines:
  origin_id      = outputLine[0].split('-')[0].strip(' ')
  dest_id   = outputLine[0].split('-')[1].split(',')
  dest_class = dest_id[0].strip(' ')
  distance  = int(round(outputLine[1]))
  place = "before chunk append of returned and processed results"
  chunkedLines.append('''('{origin_id}','{dest_class}',{distance})'''.format(origin_id = origin_id,
                                                                            dest_class = dest_class,
                                                                            distance  = distance))
place = "before execute returned results sql"
sql_query = '''{insert1}{hex}{insert2}{values}{insert3}{insert4}'''.format(insert1 = insert1,
                                                                            hex = hex,
                                                                            insert2 = insert2,
                                                                            values   = ','.join(chunkedLines),
                                                                            insert3 = insert3,
                                                                            insert4 = insert4)
curs.execute(sql_query)
place = "before commit of returned and processed results"
conn.commit()
# Where results don't exist for a destination class, ensure a null array is recorded
null_dest_insert = '''
 INSERT INTO {table} ({id}, hex, dest_class, distances)  
 SELECT gnaf_pid,{hex}, '{dest_class}', '{curlyo}{curlyc}'::int[] 
   FROM parcel_dwellings p
 WHERE hex_id = {hex}
   AND NOT EXISTS (SELECT 1 FROM {table} o 
                    WHERE dest_class = '{dest_class}' 
                      AND hex = {hex}
                      AND p.{id} = o.{id});
 '''.format(table = result_table,
            id = origin_pointsID.lower(), 
            hex = hex,
            curlyo = '{',
            curlyc = '}',
            dest_class = dest_class)   
# print(null_dest_insert)                          
curs.execute(null_dest_insert)
conn.commit()       
