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
 26 February 2019
 '''
print(about)

import time
import os
import sys
import subprocess as sp
from datetime import datetime
import networkx as nx
import osmnx as ox
ox.config(use_cache=True, log_console=True)
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
             
# iterate of files within root or otherwise specified directory, noting all poly files

# location of source OSM file
osm_dir = 'D:/osm/planet_archives/planet-latest_20181001.osm.pbf'

# location of boundary files to iterate over
search_dir = 'D:/ntnl_li_2018_template/data/study_region/'

# conversion settings
exe = 'osmconvert64-0.8.8p.exe'
exepath = 'D:/osm/'
osm_format = 'osm'

# output suffix
suffix = '_20181001'

count = 0
# Start timing the code
start_time = datetime.now()
for root, dirs, files in os.walk(search_dir):
    for file in files:
        if file.endswith(".poly"):
           # Extract OSM
           subtime = datetime.now()
           fullfile = os.path.join(root,file)
           filename = os.path.splitext(file)[0]
           studyregion = '{root}/{filename}{suffix}.{osm_format}'.format(root = root,
                                                                         filename = filename,
                                                                         suffix = suffix,
                                                                         osm_format = osm_format)
           if os.path.isfile('{}'.format(studyregion)):
             print('.osm file {} already exists'.format(studyregion))
            
           if not os.path.isfile('{}'.format(studyregion)):
             command = '{osmconvert} {osm} -B={poly} -o={studyregion}'.format(osmconvert = exe, 
                                                                            osm = osm_dir,
                                                                            poly = fullfile,
                                                                            studyregion = studyregion)
             sp.call(command, shell=True, cwd=exepath)
             count+=1
             print(' Extraction of .osm file for {} complete in {:.1f} minutes.'.format(filename,
                                                                                  (datetime.now() - subtime).total_seconds()/60))
            
print('\nExtracted (or attempted to extract) {} OSM portions.'.format(count))            
print("Elapsed time was {:.1f} minutes".format((datetime.now() - start_time).total_seconds()/60.0))