# Purpose: Prepare POS for ntnl liveability indicators
#           -- *** Assumes already in correct projection for project (e.g. GDA2020 GA LCC) *** 
#           -- copies features within study region to project gdb
#           -- calculates geodesic area in hectares
#           -- makes temporary line feature from polygons
#           -- traces vertices at set interval (pos_vertices in config file)
#
# This makes use of the 'CreatePointsLines' toolbox authored by Ian Broad
# http://ianbroad.com/arcgis-toolbox-create-points-polylines-arcpy/
#
# Author:  Carl Higgs
# Date:    20180626


import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import arcpy
import time
import psycopg2
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Clip address to study region, dissolve by location counting collapse degree'

# ArcGIS environment settings
arcpy.env.workspace = gdb_path  
arcpy.ImportToolbox(os.path.join(folderPath,CreatePointsLines_tbx))

# Define make reduced feature layer method
def renameSkinny(is_geo, in_obj, out_obj, keep_fields_list=[''], rename_fields_list=None, where_clause=''):
  ''' Make an ArcGIS Feature Layer or Table View, containing only the fields
      specified in keep_fields_list, using an optional SQL query. Default
      will create a layer/view with NO fields. Method amended (Carl 17 Nov 2016) to include a rename clause - all fields supplied in rename must correspond to names in keep_fields'''
  field_info_str = ''
  input_fields = arcpy.ListFields(in_obj)
  if not keep_fields_list:
      keep_fields_list = []
  i = 0
  for field in input_fields:
      if field.name in keep_fields_list:
          possibleNewName = (rename_fields_list[i],field.name)[rename_fields_list==None]
          field_info_str += field.name + ' ' + possibleNewName + ' VISIBLE;'
          i += 1
      else:
          field_info_str += field.name + ' ' + field.name + ' HIDDEN;'
  field_info_str.rstrip(';')  # Remove trailing semicolon
  if is_geo:
      arcpy.MakeFeatureLayer_management(in_obj, out_obj, where_clause, field_info=field_info_str)
  else:
      arcpy.MakeTableView_management(in_obj, out_obj, where_clause, field_info=field_info_str)
  return out_obj			 

# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(os.path.join(temp,db)):
    os.makedirs(os.path.join(temp,db))
    
arcpy.env.scratchWorkspace = os.path.join(temp,db)  
arcpy.env.overwriteOutput = True 

temp_pos_line = os.path.join(arcpy.env.scratchGDB,"pos_line")
temp_pos_points = os.path.join(arcpy.env.scratchGDB,"pos_points")

print("Copying POS features within study region to database..."),
arcpy.MakeFeatureLayer_management(pos_source, 'feature') 
arcpy.SelectLayerByLocation_management('feature', 'intersect',"gccsa_2016")
arcpy.CopyFeatures_management('feature', "pos_shape")
print(" Done.")

print("Calculating geodesic area in hectares..."),
arcpy.AddField_management("pos_shape", "area_ha", "DOUBLE")

arcpy.CalculateField_management("pos_shape", "area_ha", "!shape.geodesicArea@hectares!", "PYTHON")
print(" Done.")

print("Creating points at {}m intervals...".format(pos_vertices)),
arcpy.PolygonToLine_management("pos_shape", temp_pos_line, "IGNORE_NEIGHBORS")

arcpy.CreatePointsLines_CreatePointsLines(Input_Polyline_Feature_Class = temp_pos_line, 
                                          Type="INTERVAL BY DISTANCE", 
                                          Starting_Location="BEGINNING", 
                                          Use_Field_to_Set_Value_="NO", 
                                          Field_with_Value="", 
                                          Distance___Percentage_Value="{}".format(pos_vertices), 
                                          Add_End_Points_="BOTH", 
                                          Output_Point_Feature_Class= temp_pos_points)
arcpy.Delete_management(temp_pos_line)
print(" Done.") 

print("Creating compound POS entry point ID..."),
arcpy.AddField_management(temp_pos_points, "pos_entryid", "TEXT")
arcpy.CalculateField_management(in_table=temp_pos_points, 
                                field="pos_entryid", 
                                expression="'{0},{1}'.format( !pos_line_ORIG_FID! ,int(math.ceil( !mem_point_Value! /50)))", 
                                expression_type="PYTHON")
print(" Done.")                                

print("Save to geodatabase with only important fields retained (OID, shape, orig_fid and pos_entry_id)..."),
field_info_str = 'OBJECTID OBJECTID VISIBLE;Shape Shape VISIBLE;mem_point_LineOID mem_point_LineOID HIDDEN;mem_point_Value mem_point_Value HIDDEN;pos_line_OBJECTID pos_line_OBJECTID HIDDEN;pos_line_Id pos_line_Id HIDDEN;pos_line_area_ha area_ha VISIBLE;pos_line_ORIG_FID pos_id VISIBLE;pos_entryid pos_entryid VISIBLE;'             
arcpy.MakeFeatureLayer_management(temp_pos_points, 'skinny_pos', '', field_info=field_info_str)            
arcpy.CopyFeatures_management('skinny_pos', 'pos_50m_vertices') 
print(" Done.")
  
# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()  
  
# gdb to pgsql
print("Copy the pos shape to PostgreSQL database..."),
command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" ' \
        + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
        + 'user={user} password = {pwd} " '.format(user = db_user,pwd = db_pwd) \
        + '{gdb} "{feature}" '.format(gdb = gdb_path,feature = "pos_shape") \
        + '-lco geometry_name="geom" '
sp.call(command, shell=True)


# Depending on whether pos_category variable is defined, this will be included in the
# pos_50m_vertices table; if included, the pos_category variable can be used to define 
# queries in the config file.
if pos_category == '':
  print("Copy the pos points with area attribute data to PostgreSQL database..."),
  command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" ' \
          + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
          + 'user={user} password = {pwd} " '.format(user = db_user,pwd = db_pwd) \
          + '{gdb} "{feature}" '.format(gdb = gdb_path,feature = "pos_50m_vertices") \
          + '-lco geometry_name="geom" '
  sp.call(command, shell=True)
  curs.execute(grant_query)
  conn.commit()

if pos_category != '':
  print("Copy the pos points with area attribute data to PostgreSQL database..."),
  command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" ' \
          + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
          + 'user={user} password = {pwd} " '.format(user = db_user,pwd = db_pwd) \
          + '{gdb} "{feature}" '.format(gdb = gdb_path,feature = "temp_pos_vertices") \
          + '-lco geometry_name="geom" '
  sp.call(command, shell=True)
  curs.execute(grant_query)
  conn.commit()
  pos_vertices_with_cat = '''
  CREATE TABLE pos_50m_vertices AS
  SELECT a.*, 
         b.{pos_cat} 
  FROM temp_pos_vertices a
  LEFT JOIN pos_shape b ON a.pos_id = b.objectid;
  '''.format(pos_cat = pos_category)
  curs.execute(pos_verticies_with_cat)
  conn.commit()
  curs.execute(grant_query)
  conn.commit()
  
conn.close()
print(" Done.") 
 
# output to completion log    
script_running_log(script, task, start, locale)
