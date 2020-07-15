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

# import folium
# from folium.plugins import MarkerCluster
# from folium.plugins import FastMarkerCluster

# from bokeh.models import ColumnDataSource

from script_running_log import script_running_log
# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd, host=db_host,port=db_port)
curs = conn.cursor()

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db))

# define pedestrian network custom filter (based on OSMnx 'walk' network type, without the cycling exclusion)
pedestrian = (
             '["area"!~"yes"]'
             '["highway"!~"motor|proposed|construction|abandoned|platform|raceway"]'
             '["foot"!~"no"]'
             '["service"!~"private"]'
             '["access"!~"private"]'
             )
             
osmnx_retain_all = 'False'
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

if os.path.isfile(os.path.join(locale_dir,
      '{studyregion}_pedestrian_{osm_prefix}.graphml'.format(studyregion = buffered_study_region,
                                                            osm_prefix = osm_prefix))):
  print('Pedestrian road network for {} has already been processed; loading this up.'.format(buffered_study_region))
  W = ox.load_graphml('{studyregion}_pedestrian_{osm_prefix}.graphml'.format(studyregion = buffered_study_region,
                                                                             osm_prefix = osm_prefix),
                      folder = locale_dir)
else:
  subtime = datetime.now()
  # # load buffered study region in EPSG4326 from postgis
  sql = '''SELECT ST_Transform(geom,4326) AS geom FROM {}'''.format(buffered_study_region)
  polygon =  gpd.GeoDataFrame.from_postgis(sql, engine, geom_col='geom' )['geom'][0]
  print('Creating and saving all roads network... '),
  W = ox.graph_from_polygon(polygon,  network_type= 'all', retain_all = osmnx_retain_all)
  ox.save_graphml(W, 
     filename='{studyregion}_all_{osm_prefix}.graphml'.format(studyregion = buffered_study_region,
                                                                            osm_prefix = osm_prefix), 
     folder=locale_dir, 
     gephi=False)
  ox.save_graph_shapefile(W, 
     filename='{studyregion}_all_{osm_prefix}'.format(studyregion = buffered_study_region,
                                                                       osm_prefix = osm_prefix),
     folder = locale_dir)
  print('Done.')
  print('Creating and saving pedestrian roads network... '),
  W = ox.graph_from_polygon(polygon,  custom_filter= pedestrian, retain_all = osmnx_retain_all)
  ox.save_graphml(W, 
                 filename='{studyregion}_pedestrian_{osm_prefix}.graphml'.format(studyregion = buffered_study_region,
                                                                                 osm_prefix = osm_prefix), 
      folder=locale_dir, 
      gephi=False)
  ox.save_graph_shapefile(W, 
      filename = '{studyregion}_pedestrian_{osm_prefix}'.format(studyregion = buffered_study_region,
                                                    osm_prefix = osm_prefix),
      folder = locale_dir)
  print('Done.')  
  
print("Copy the network edges and nodes from shapefiles to Postgis..."),
curs.execute('''SELECT 1 WHERE to_regclass('public.edges') IS NOT NULL AND to_regclass('public.nodes') IS NOT NULL;''')
res = curs.fetchone()
if res is None:
    for feature in ['edges','nodes']:
        command = (
                ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
                ' PG:"host={host} port={port} dbname={db}'
                ' user={user} password = {pwd}" '
                ' {dir}/{studyregion}_pedestrian_{osm_prefix}/{feature}/{feature}.shp '
                ' -lco geometry_name="geom"'.format(host = db_host,
                                                    port=db_port,
                                                    db = db,
                                                    user = db_user,
                                                    pwd = db_pwd,
                                                    dir = locale_dir,
                                                    studyregion = buffered_study_region,
                                                    osm_prefix = osm_prefix,
                                                    feature = feature) 
                )
        print(command)
        sp.call(command, shell=True)
    print("Done (although, if it didn't work you can use the printed command above to do it manually)")  
else:
    print("  - It appears that pedestrian network edges and nodes have already been exported to Postgis.")  

# Copy clean intersections to postgis
intersections_table = 'clean_intersections_12m'
print("Prepare and copy clean intersections to postgis... ")
curs.execute('''SELECT 1 WHERE to_regclass('public.{}') IS NOT NULL;'''.format(intersections_table))
res = curs.fetchone()
if res is None:
    # Clean intersections
    G_proj = ox.project_graph(W)
    intersections = ox.clean_intersections(G_proj, tolerance=intersection_tolerance, dead_ends=False)
    intersections.crs = G_proj.graph['crs']
    intersections_latlon = intersections.to_crs(epsg=4326)
    sql = '''
    DROP TABLE IF EXISTS {table};
    CREATE TABLE {table} (point_4326 geometry);
    INSERT INTO {table} (point_4326) VALUES {points};
    ALTER TABLE {table} ADD COLUMN geom geometry;
    UPDATE {table} SET geom = ST_Transform(point_4326,{srid});
    ALTER TABLE {table} DROP COLUMN point_4326;
    '''.format(table = intersections_table,
            points = ', '.join(["(ST_GeometryFromText('{}',4326))".format(x.wkt) for x in intersections_latlon]),
            srid = srid)  
    engine.execute(sql)      
    print("  - Done.")
else:
    print("  - It appears that clean intersection data has already been prepared and imported for this region.")  

# Create sample points
"""print("Create sample points at regular intervals along the network... ")
curs.execute('''SELECT 1 WHERE to_regclass('public.{}') IS NOT NULL;'''.format(points))
res = curs.fetchone()
if res is None:
    sql = '''
    CREATE TABLE {table} AS
    WITH line AS 
            (SELECT
                ogc_fid,
                (ST_Dump(ST_Transform(geom,32647))).geom AS geom
            FROM edges),
        linemeasure AS
            (SELECT
                ogc_fid,
                ST_AddMeasure(line.geom, 0, ST_Length(line.geom)) AS linem,
                generate_series(0, ST_Length(line.geom)::int, {interval}) AS metres
            FROM line),
        geometries AS (
            SELECT
                ogc_fid,
                metres,
                (ST_Dump(ST_GeometryN(ST_LocateAlong(linem, metres), 1))).geom AS geom 
            FROM linemeasure)
    SELECT
        row_number() OVER() AS point_id,
        ogc_fid,
        metres,
        ST_SetSRID(ST_MakePoint(ST_X(geom), ST_Y(geom)), {srid}) AS geom
    FROM geometries;
    '''.format(table = points,
               interval = point_sampling_interval,
               srid = srid)  
    engine.execute(sql)      
    engine.execute(grant_query)      
    print("  - Sampling points table {} created with sampling at every {} metres along the pedestrian network.".format(points,point_sampling_interval))
else:
    print("  - It appears that sample points table {} have already been prepared for this region.".format(points))  
"""    
    
# # get map data
# map_layers={}

# sql = '''
# SELECT '10km study region buffer' AS "Description",
       # ST_Transform(geom,4326) geom 
# FROM {}
# '''.format(buffered_study_region)
# map_layers['buffer'] = gpd.GeoDataFrame.from_postgis(sql, engine, geom_col='geom' )

# sql = '''
# SELECT ST_Transform(geom,4326) geom
# FROM {table};
# '''.format(table = points) 
# map_layers['sampling_points'] = gpd.GeoDataFrame.from_postgis(sql, engine, geom_col='geom')

# sql = '''
# SELECT ST_Transform(geom,4326) geom
# FROM {table};
# '''.format(table = 'edges') 
# map_layers['edges'] = gpd.GeoDataFrame.from_postgis(sql, engine, geom_col='geom')

# xy = [float(map_layers['buffer'].centroid.y),float(map_layers['buffer'].centroid.x)]  
# bounds = map_layers['buffer'].bounds.transpose().to_dict()[0]

# # initialise map
# m = folium.Map(location=xy, zoom_start=11,tiles=None, control_scale=True, prefer_canvas=True)
# m.add_tile_layer(tiles='Stamen Toner',name='simple map', overlay=True,active=True)
# # add layers (not true choropleth - for this it is just a convenient way to colour polygons)
# buffer = folium.Choropleth(map_layers['buffer'].to_json(),
                           # name='10km study region buffer',
                           # fill_color=colours['qualitative'][1],
                           # fill_opacity=0,
                           # line_color=colours['qualitative'][1], 
                           # highlight=False).add_to(m)

# # feature = folium.Choropleth(map_layers[areas[0]['name_s']].to_json(),
                            # # name=str.title(areas[0]['name_f']),
                            # # fill_opacity=0,
                            # # line_color=colours['qualitative'][0], 
                            # # highlight=False).add_to(m)
                            
# # folium.features.GeoJsonTooltip(fields=['Subdistrict',population_field],
                               # # labels=True, 
                               # # sticky=True
                              # # ).add_to(feature.geojson)
                              
# # add clustered markers for intersections
# locations = list(zip(map_layers['sampling_points'].centroid.y, map_layers['sampling_points'].centroid.x))
# # cluster = folium.MarkerCluster(locations=locations, icons=icons, popups=popups)
# # m.add_child(cluster)

# callback = ('function (row) {' 
            # 'var circle = L.circle(new L.LatLng(row[0], row[1],{color: "red", radius: 20000}));'
            # 'return circle'
            # '};')
# m.add_child(FastMarkerCluster(locations, 
                              # callback=callback, 
                              # name = 'Sample points ({}m interval)'.format(point_sampling_interval)))
# folium.PolyLine(map_layers['edges'].loc[0,'geom'].coords[:]).add_to(m)  
# folium.LayerControl(collapsed=True).add_to(m)

# # checkout https://nbviewer.jupyter.org/gist/jtbaker/57a37a14b90feeab7c67a687c398142c?flush_cache=true
# # save map
# map_name = '{}_04_network.html'.format(locale)
# m.save('{}/html/{}.html'.format(locale_maps,map_name))
# m.save('../maps/{}'.format(map_name))
# print("\nPlease inspect results using interactive map saved in project maps folder: {}\n".format(map_name))          
    
script_running_log(script, task, start)

# clean up
conn.close()
