# Script:  create_pedestrian_networks.py
# Purpose: Create pedestrian street networks for specified city (2019)
# Author:  Carl Higgs 
# Date:    20190226

about = '''
Pedestrian street networks for specified city (2019)
 Using OpenStreetMap as a source for 
 * complete road network, and 
 * a pedestrian 'walk/cycle'network. 
 * calculating intersection density for the pedestrian network
 
 The process is undertaken for the specified city.
 
 This script is adapted based on 'OSMnx - 21 Cities.ipynb'; created for the 21 cities framework.  
 In the first instance, this script has been developed for Mildura LGA.  However, it is not fully
 integrated in the broader scripted process; it has certain environment assumptions specifically
 relating to OSMnx dependencies which are not accounted for in the 2018 National Liveability process.
 Hence, for the 21 cities project I undertook this processing on my machine using the Jupyter notebook
 (referenced above).  Ultimately, a future update of the scripted process should ensure that the 
 appropriate virtual environment required for running all scripts is set up in the first instance.
 
 In short -- Carl has a virtual environment set up for OSMnx on his computer that currently others do not.
 We should change this in the medium term, so all can run this script.
 
 Carl Higgs
 26 February 2019pip 
 
 EXTRA BITS:
 To set up the environment some of the following could be used eventually:
    conda create --override-channels -c conda-forge -n ntnl_li ^
                 python=3                                      ^ 
                 gdal vs2015_runtime=14                        ^
                 osmnx=0.8.2                                   ^
                 rasterstats=0.13.0                            ^
                 geoalchemy2=0.5.0
    activate ntnl_li
    pip install altair
    pip install rasterio
    pip install osmnx
    pip install rasterstats
    pip install xlrd
    pip install geoalchemy2
    conda upgrade numpy
 '''
print(about)

import time
import os
import sys
import subprocess as sp
from datetime import datetime
import networkx as nx
import osmnx as ox
ox.config(use_cache=True, log_console=False)
import requests
import fiona
from shapely.geometry import shape, MultiPolygon, Polygon
# from sqlalchemy import *
from sqlalchemy import create_engine
from geoalchemy2 import Geometry
from script_running_log import script_running_log
# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'


# define pedestrian network custom filter (based on OSMnx 'walk' network type, without the cycling exclusion)
pedestrian = (
             '["area"!~"yes"]' 
             '["highway"!~"motor|proposed|construction|abandoned|platform|raceway"]'
             '["foot"!~"no"]'  
             '["service"!~"private"]' 
             '["access"!~"private"]'
             )
             

# location of buffered boundary file
locale_4326_shp = os.path.join(locale_dir,'{}_{}_{}m_epsg4326.shp'.format(locale.lower(),study_region,study_buffer))
locale_stub = os.path.splitext(locale_4326_shp)[0]

# conversion settings
osm_format = 'osm'

# output suffix
suffix = osm_prefix.strip('osm')

print("Create poly file, using command:")
command = 'python ogr2poly.py {feature}'.format(feature = locale_4326_shp)
print('\t{}'.format(command))
sp.call(command, shell=True)
print('Done')
print('Store poly file in study region folder, using command: ')
locale_poly = '{}.poly'.format(locale_stub)
command = 'move {old_place} {proper_place}'.format(old_place = os.path.split('{}_0.poly'.format(locale_stub))[1],
                                                 proper_place = locale_poly)
print('\t{}'.format(command))
sp.call(command, shell=True)
print("Done.")

print("Extract OSM for studyregion... "),
studyregion = '{locale_stub}{suffix}.{osm_format}'.format(locale_stub = locale_stub,
                                                              suffix = suffix,
                                                              osm_format = osm_format)
if os.path.isfile('{}'.format(studyregion)):
  print('.osm file {} already exists'.format(studyregion))
 
if not os.path.isfile('{}'.format(studyregion)):
  command = '{osmconvert} {osm} -B={poly} -o={studyregion}'.format(osmconvert = osmconvert, 
                                                                 osm = osm_data,
                                                                 poly = locale_poly,
                                                                 studyregion = studyregion)
  sp.call(command, shell=True)
  
print('Done.')

print('Get networks and save as graphs (retain_all = False*** ie. only main network segment is retained).  Ensure this is appropriate for your study region.  In future, we could make this an optional parameter (false by default, but true if a region is known to have islands.  The problem with network islands is that, in many cases they are artifacts of cartography, not true islands.')
retain_all = False
root,filename = os.path.split(locale_stub)

if os.path.isfile(os.path.join(root,
      'osm_{studyregion}_pedestrian{suffix}.graphml'.format(studyregion = filename,
                                                            suffix = suffix))):
  print('Pedestrian road network for {} has already been processed; loading this up.'.format(filename))
  W = ox.load_graphml(os.path.join(root,
      'osm_{studyregion}_pedestrian{suffix}.graphml'.format(studyregion = filename,
                                                            suffix = suffix)))
else:
  subtime = datetime.now()
  # Extract pedestrian network
  c = fiona.open(locale_4326_shp)   
  polygon = shape(next(iter(c))['geometry'])
  print('Creating and saving all roads network... '),
  W = ox.graph_from_polygon(polygon,  network_type= 'all', retain_all = retain_all)
  ox.save_graphml(W, 
     filename=os.path.join(root,
                           'osm_{studyregion}_all{suffix}.graphml'.format(studyregion = filename,
                           suffix = suffix)), 
     folder=None, 
     gephi=False)
  ox.save_graph_shapefile(W, 
     filename=os.path.join(root,
                           'osm_{studyregion}_all{suffix}'.format(studyregion = filename,
                                                                       suffix = suffix)))
  print('Done.')
  print('Creating and saving pedestrian roads network... '),
  W = ox.graph_from_polygon(polygon,  custom_filter= pedestrian, retain_all = retain_all)
  ox.save_graphml(W, filename=os.path.join(root,
      'osm_{studyregion}_pedestrian{suffix}.graphml'.format(studyregion = filename,
                                                            suffix = suffix)), 
      folder=None, 
      gephi=False)
  ox.save_graph_shapefile(W, 
      filename=os.path.join(root,
      'osm_{studyregion}_pedestrian{suffix}'.format(studyregion = filename,
                                                    suffix = suffix)))
  print('Done.')                                                            

# Clean intersections
print("Prepare cleaned intersections... "),

G_proj = ox.project_graph(W)
intersections = ox.clean_intersections(G_proj, tolerance=12, dead_ends=False)
intersections.crs = G_proj.graph['crs']
intersections_latlon = intersections.to_crs(epsg=4326)
intersections_table = "clean_intersections_12m"
# to sql  - works well!
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db))
conn = engine.connect()
statement = '''
  DROP TABLE IF EXISTS {table};
  CREATE TABLE {table} (point_4326 geometry);
  INSERT INTO {table} (point_4326) VALUES {points};
  ALTER TABLE {table} ADD COLUMN geom geometry;
  UPDATE {table} SET geom = ST_Transform(point_4326,{srid});
  ALTER TABLE {table} DROP COLUMN point_4326;
'''.format(table = intersections_table,
           points = ', '.join(["(ST_GeometryFromText('{}',4326))".format(x.wkt) for x in intersections_latlon]),
           srid = srid)  
conn.execute(statement)      
print("Done.")
  
# Copy joined, cropped Urban Metro meshblock + dwellings feature from postgis to project geodatabase
arcpy.env.workspace = db_sde_path
arcpy.CopyFeatures_management('public.{}'.format(intersections_table), 
                               os.path.join(gdb_path,intersections_table))  
  
# output to completion log    
script_running_log(script, task, start)

# clean up
conn.close()
