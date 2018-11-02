# Import VPA and FOI POS layers for Melb to a postgresql database
# Author:  Carl Higgs
# Date:    20181101

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
task = 'Import OSM'

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

## Sort out the network
# ArcGIS environment settings
arcpy.env.workspace = gdb_path  
arcpy.env.overwriteOutput = True 
SpatialReference = arcpy.SpatialReference(SpatialRef)

pos_source = 'D:/ntnl_li_2018_template/data/destinations/pos_2018.gdb'
# note that these names are important; 
# the suffix (chunk after '_') is used to name features
# pertaining to that datasource
pos_layers = ['melb_foi','melb_vpa']

## Using GDA2020 edges and nodes derived by Bec as pedestrian network from VicMap data 20181101
network_gdb_source = 'D:/ntnl_li_2018_template/data/study_region/melb/VicMapRds_Oct2018_Pedestrian.gdb'
network_source_feature_dataset = 'pedestrian_vicmap'

print("Creating feature dataset to hold network..."),
if not arcpy.Exists("pedestrian_vicmap"):
  arcpy.CreateFeatureDataset_management(gdb_path,
                                      network_source_feature_dataset, 
                                      spatial_reference = SpatialReference)
  for feature in ['edges_vicmap','nodes_vicmap']:
    print("Copy {} to feature dataset {}...".format(feature,os.path.join(gdb_path,network_source_feature_dataset))),
    arcpy.Copy_management(os.path.join(network_gdb_source,feature), os.path.join(gdb_path,network_source_feature_dataset,feature))
    print(" Done.")
    
  arcpy.CheckOutExtension('Network')
  # # The below process assumes a network dataset template has been created
  # # This was achieved for the current OSMnx schema with the below code
  # arcpy.CreateTemplateFromNetworkDataset_na(network_dataset="D:/ntnl_li_2018_template/data/li_melb_2016_osmnx.gdb/PedestrianRoads/PedestrianRoads_ND", 
                                            # output_network_dataset_template="D:/ntnl_li_2018_template/data/roads/osmnx_nd_template.xml")
  # NOTE: for the vicmap test, i modified the template replacing the word "PedestrianRoads" with "pedestrian_vicmap"
  # I think this should work, as otherwise all data is the same format-wise (roads are 'edges', intersections are 'nodes', projection is same, extent is same)
  print("Creating network dataset from template..."),                                          
  arcpy.CreateNetworkDatasetFromTemplate_na(os.path.join(folderPath,'study_region/melb/osmnx_nd_template_vicmap.xml'), 
                                            network_source_feature_dataset)
  print(" Done.")
                        
  print("Build network..."),                  
  arcpy.BuildNetwork_na(os.path.join(network_source_feature_dataset,'{}_ND'.format(network_source_feature_dataset)))
  print(" Done.")
  arcpy.CheckInExtension('Network')  

print("Copy the network edges from gdb to postgis..."),
command = (
        ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
        ' PG:"host={host} port=5432 dbname={db}'
        ' user={user} password = {pwd}" '
        ' {gdb} "{feature}" '
        ' -lco geometry_name="geom" '.format(host = db_host,
                                     db = db,
                                     user = db_user,
                                     pwd = db_pwd,
                                     gdb = gdb_path,
                                     feature = 'edges_vicmap') 
        )
print(command)
sp.call(command, shell=True)
print("Done (although, if it didn't work you can use the printed command above to do it manually)")

## Sort out the POS layers
arcpy.env.workspace = db_sde_path
for layer in pos_layers:
  pos = layer.split('_')[1]
  print("Copy the {} from gdb to postgis...".format(layer)),
  command = (
        ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
        ' PG:"host={host} port=5432 dbname={db}'
        ' user={user} password = {pwd}" '
        ' {gdb} "{feature}" '
        ' -lco geometry_name="geom"'.format(host = db_host,
                                     db = db,
                                     user = db_user,
                                     pwd = db_pwd,
                                     gdb = pos_source,
                                     feature = layer) 
        )
  print(command)
  sp.call(command, shell=True)
  print("Done (although, if it didn't work you can use the printed command above to do it manually)")

  # connect to the PostgreSQL server and ensure privileges are granted for all public tables
  curs.execute(grant_query)
  conn.commit()

  pos_setup = ['''
  ALTER TABLE {layer} RENAME COLUMN objectid TO aos_id;
  '''.format(layer = layer),
  '''
  -- Create a linestring pos table 
  DROP TABLE IF EXISTS {pos}_line;
  CREATE TABLE {pos}_line AS 
  WITH pos_bounds AS
    (SELECT aos_id, ST_SetSRID(st_astext((ST_Dump(geom)).geom),7845) AS geom  FROM {layer})
  SELECT aos_id, ST_Length(geom)::numeric AS length, geom    
  FROM (SELECT aos_id, ST_ExteriorRing(geom) AS geom FROM pos_bounds) t;
  '''.format(pos = pos,layer = layer),
  '''
  -- Generate a point every 20m along a park outlines: 
  DROP TABLE IF EXISTS {pos}_nodes; 
  CREATE TABLE {pos}_nodes AS 
   WITH pos AS 
   (SELECT aos_id, 
           length, 
           generate_series(0,1,20/length) AS fraction, 
           geom FROM {pos}_line) 
  SELECT aos_id,
         row_number() over(PARTITION BY aos_id) AS node, 
         ST_LineInterpolatePoint(geom, fraction)  AS geom 
  FROM pos;
  CREATE INDEX {pos}_nodes_idx ON {pos}_nodes USING GIST (geom);
  ALTER TABLE {pos}_nodes ADD COLUMN aos_entryid varchar; 
  UPDATE {pos}_nodes SET aos_entryid = aos_id::text || ',' || node::text; 
  '''.format(pos = pos),
  '''
  -- Create table of points within 30m of OSM network
  -- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
  DROP TABLE IF EXISTS {pos}_nodes_30m_osm;
  CREATE TABLE {pos}_nodes_30m_osm AS 
  SELECT DISTINCT n.* 
  FROM {pos}_nodes n, 
       edges l
  WHERE ST_DWithin(n.geom ,l.geom,30);
  '''.format(pos = pos, layer = layer),
  '''
  -- Create table of points within 30m of VicMap network
  -- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
  DROP TABLE IF EXISTS {pos}_nodes_30m_vicmap;
  CREATE TABLE {pos}_nodes_30m_vicmap AS 
  SELECT DISTINCT n.* 
  FROM {pos}_nodes n, 
       edges_vicmap l
  WHERE ST_DWithin(n.geom ,l.geom,30);
  '''.format(pos = pos,layer = layer,osm_prefix = osm_prefix)
  ]
  for sql in pos_setup:
    start = time.time()
    print("\nExecuting: {}".format(sql))
    curs.execute(sql)
    conn.commit()
    print("Executed in {} mins".format((time.time()-start)/60))
    
  # pgsql to gdb
  print("Copying nodes for {} from postgis to gdb".format(pos)),
  arcpy.CopyFeatures_management('public.{pos}_nodes_30m_osm'.format(pos = pos), 
                                 os.path.join(gdb_path,'{pos}_nodes_30m_osm'.format(pos = pos))) 
  arcpy.CopyFeatures_management('public.{pos}_nodes_30m_vicmap'.format(pos = pos), 
                                 os.path.join(gdb_path,'{pos}_nodes_30m_vicmap'.format(pos = pos))) 
  print("Done.")
  
## additional queries for already processed osm to make sure all features have like names
additiona_osm_queries = ['''
-- Create table of points within 30m of lines (should be your road network) 
-- Note - this is identical to feature created in 13_aos_setup.py as aos_nodes_30m_line 
DROP TABLE IF EXISTS osm_nodes_30m_osm;
CREATE TABLE osm_nodes_30m_osm AS 
SELECT * 
  FROM aos_nodes_30m_line ;
''',
'''
-- Create table of points within 30m of lines (should be your road network) 
-- Distinct is used to avoid redundant duplication of points where they are within 20m of multiple roads 
DROP TABLE IF EXISTS osm_nodes_30m_vicmap;
CREATE TABLE osm_nodes_30m_vicmap AS 
SELECT DISTINCT n.* 
FROM aos_nodes n, 
     edges_vicmap l
WHERE ST_DWithin(n.geom_w_schools ,l.geom,30);
''']
for sql in additiona_osm_queries:
    start = time.time()
    print("\nExecuting: {}".format(sql))
    curs.execute(sql)
    conn.commit()
    print("Executed in {} mins".format((time.time()-start)/60))  
    
# pgsql to gdb
print("Copying nodes for OSM from postgis to gdb"),
arcpy.CopyFeatures_management('public.osm_nodes_30m_osm', 
                               os.path.join(gdb_path,'osm_nodes_30m_osm')) 
arcpy.CopyFeatures_management('public.osm_nodes_30m_vicmap', 
                               os.path.join(gdb_path,'osm_nodes_30m_vicmap')) 
print("Done")
# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
 