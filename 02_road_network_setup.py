# Script:  create_pedestrian_networks.py
# Purpose: Create pedestrian street networks for specified city (2019)
# Author:  Carl Higgs 
# Date:    20190226

import time
import os
import sys
import subprocess as sp
from datetime import datetime
import psycopg2 
import networkx as nx
import osmnx as ox
ox.config(use_cache=True, log_console=True)
from shapely.geometry import shape, MultiPolygon, Polygon
import geopandas as gpd
from sqlalchemy import create_engine
from geoalchemy2 import Geometry, WKTElement

from script_running_log import script_running_log
# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

engine = create_engine(f"postgresql://{db_user}:{db_pwd}@{db_host}/{db}")

print("Get networks and save as graphs.")
if osmnx_retain_all == 'False':
    osmnx_retain_all = False
    print('''
    Note: "retain_all = False" ie. only main network segment is retained.
        Please ensure this is appropriate for your study region 
        (ie. networks on real islands may be excluded).
    ''') 
else:
    osmnx_retain_all = True
    print('''
    Note: "retain_all = True" ie. all network segments will be retained.
        Please ensure this is appropriate for your study region 
        (ie. networks on real islands will be included, however network 
        artifacts resulting in isolated network segments, or network islands,
        may also exist.  These could be problematic if sample points are 
        snapped to erroneous, mal-connected segments.  Check results.).
    ''') 

if os.path.isfile(f'{locale_dir}/{network_source}.graphml'):
  print(f'Pedestrian road network for {buffered_study_region} has already been processed; loading this up.')
  W = ox.load_graphml(f'{locale_dir}/{network_source}.graphml')
else:
  subtime = datetime.now()
  # # load buffered study region in EPSG4326 from postgis
  sql = f'''SELECT ST_Transform(geom,4326) AS geom FROM {buffered_study_region}'''
  polygon =  gpd.GeoDataFrame.from_postgis(sql, engine, geom_col='geom' )['geom'][0]
  print('Creating and saving all roads network... '),
  W = ox.graph_from_polygon(polygon,  network_type= 'all', retain_all = osmnx_retain_all)
  network_all = network_source.replace('pedestrian','all')
  ox.save_graphml(W, filepath = f'{locale_dir}/{network_all}.graphml', gephi=False)
  ox.save_graph_shapefile(W, filepath = f'{locale_dir}/{network_all}')
  print('Done.')
  print('Creating and saving pedestrian roads network... '),
  # we state network_type='walk' to ensure that the graphml graph is bidirectional, should it be used
  W = ox.graph_from_polygon(polygon,  custom_filter= pedestrian, retain_all = osmnx_retain_all, network_type='walk')
  ox.save_graphml(W, filepath = f'{locale_dir}/{network_source}.graphml', gephi=False)
  ox.save_graph_shapefile(W, filepath = f'{locale_dir}/{network_source}')
  print('Done.')  
  
print("Copy the network edges and nodes from shapefiles to Postgis..."),

for feature in ['edges','nodes']:
    # Note: prior to 1 June 2020, the edge and note features were stored in this dir,
    #  f' {locale_dir}/{buffered_study_region}_pedestrian_{osm_prefix}/{feature}/{feature}.shp '
    # ie. in a folder of name 'edges' or 'nodes'; this is no longer so
    if not engine.dialect.has_table(engine, feature,network_schema):  
        command = (
                ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
                f' PG:"host={db_host} port={db_port} dbname={db} active_schema={network_schema}'
                f' user={db_user} password = {db_pwd}" '
                f' {locale_dir}/{buffered_study_region}_pedestrian_{osm_prefix}/{feature}.shp '
                 ' -lco geometry_name="geom"'
                )
        print(command)
        sp.call(command, shell=True)
        print("Done (although, if it didn't work you can use the printed command above to do it manually)")  
    else:
        print(f"Using pedestrian network {feature} features previously exported to Postgis.")  

# Copy clean intersections to postgis
print("Prepare and copy clean intersections to postgis... ")
if not engine.dialect.has_table(engine, intersections_table,network_schema):  
    # Clean intersections
    G_proj = ox.project_graph(W)
    intersections = ox.clean_intersections(G_proj, tolerance=intersection_tolerance, dead_ends=False)
    intersections.crs = G_proj.graph['crs']
    intersections_latlon = intersections.to_crs(epsg=4326)
    points = ', '.join(["(ST_GeometryFromText('{}',4326))".format(x.wkt) for x in intersections_latlon])
    sql = f'''
    DROP TABLE IF EXISTS {intersections_table};
    CREATE TABLE {intersections_table} (point_4326 geometry);
    INSERT INTO {intersections_table} (point_4326) VALUES {points};
    ALTER TABLE {intersections_table} ADD COLUMN geom geometry;
    UPDATE {intersections_table} SET geom = ST_Transform(point_4326,{srid});
    ALTER TABLE {intersections_table} DROP COLUMN point_4326;
    '''
    engine.execute(sql)      
    print("  - Done.")
else:
    print("  - It appears that clean intersection data has already been prepared and imported for this region.")  

# # Create sample points - note, not applicable for the National Liveability project 
# # as at 1 June 2020; G-NAF points are used for basis of sampling still at this stage.
# print("Create sample points at regular intervals along the network... ")
# if not engine.dialect.has_table(engine, points):  
#     sql = '''
#     CREATE TABLE {table} AS
#     WITH line AS 
#             (SELECT
#                 ogc_fid,
#                 (ST_Dump(ST_Transform(geom,32647))).geom AS geom
#             FROM edges),
#         linemeasure AS
#             (SELECT
#                 ogc_fid,
#                 ST_AddMeasure(line.geom, 0, ST_Length(line.geom)) AS linem,
#                 generate_series(0, ST_Length(line.geom)::int, {interval}) AS metres
#             FROM line),
#         geometries AS (
#             SELECT
#                 ogc_fid,
#                 metres,
#                 (ST_Dump(ST_GeometryN(ST_LocateAlong(linem, metres), 1))).geom AS geom 
#             FROM linemeasure)
#     SELECT
#         row_number() OVER() AS point_id,
#         ogc_fid,
#         metres,
#         ST_SetSRID(ST_MakePoint(ST_X(geom), ST_Y(geom)), {srid}) AS geom
#     FROM geometries;
#     '''.format(table = points,
#                interval = point_sampling_interval,
#                srid = srid)  
#     engine.execute(sql)      
#     engine.execute(grant_query)      
#     print(f"  - Sampling points table {points} created with sampling at every {point_sampling_interval} metres along the pedestrian network.")
# else:
#     print("  - It appears that sample points table {points} have already been prepared for this region.")   
    


# Set up road network for study region using edges and nodes generated 
# from Openstreetmap data using OSMnx.  
#
# it 
# - assumes a template network dataset has been constructed.
# - assumes use of ArcGIS Pro 2.5
# - projects the source nodes and edges from WGS84 to GDA2020 GA LCC
#   (or other projection type, as req'd depending on configuration file set up)

import arcpy

# ArcGIS environment settings
arcpy.env.workspace = gdb_path  
arcpy.env.overwriteOutput = True 
SpatialReference = arcpy.SpatialReference(SpatialRef)

## NOTE This projection method will be scenario specific
# Using OSMnx data we project from 
# WGS84 to GDA2020 GA LCC using NTv2 transformation grid

print("Creating feature dataset to hold network..."),
arcpy.CreateFeatureDataset_management(gdb_path,
                                      network_source_feature_dataset, 
                                      spatial_reference = SpatialReference)
print(" Done.")

for feature in ['edges','nodes']:
  print("Project {} to feature dataset in {}...".format(feature,SpatialRef)),
  # note that previous project version stored edge / node features in folders 'edges' or 'nodes'
  arcpy.Project_management(in_dataset = f'{network_source}/{feature}.shp',
                out_dataset=f'{network_source_feature_dataset}/{feature}',
                out_coor_system = out_coor_system, 
                transform_method = network_transform_method, 
                in_coor_system = network_in_coor_system, 
                preserve_shape="NO_PRESERVE_SHAPE", 
                max_deviation="", 
                vertical="NO_VERTICAL")
  print(" Done.")

arcpy.CheckOutExtension('Network')
# # The below process assumes a network dataset template has been created
# # This was achieved for the current OSMnx schema with the below code
# arcpy.CreateTemplateFromNetworkDataset_na(network_dataset="D:/ntnl_li_2018_template/data/li_melb_2016_osmnx.gdb/PedestrianRoads/PedestrianRoads_ND", 
                                          # output_network_dataset_template="D:/ntnl_li_2018_template/data/roads/osmnx_nd_template.xml")
print("Creating network dataset from template..."),                                          
arcpy.CreateNetworkDatasetFromTemplate_na(network_template, 
                                          network_source_feature_dataset)
print(" Done.")
                        
# build the road network       
print("Build network..."),                  
arcpy.BuildNetwork_na(in_network_dataset)
print(" Done.")
arcpy.CheckInExtension('Network')  

script_running_log(script, task, start)

# clean up
engine.dispose()
