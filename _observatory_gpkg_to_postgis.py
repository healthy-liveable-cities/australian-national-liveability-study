# Script:  _observatory_gpkg_to_postgis.py
# Purpose: Create area level indicator tables
# Author:  Carl Higgs 
# Date:    20 July 2018

# Need to first:
# CREATE DATABASE observatory;
# CREATE EXTENSION postgis;

# Afterwords need to:
# pg_dump -U postgres -h localhost -W   observatory > observatory.sql

# and some time refactor this code to do the above!

import os
import sys
import time
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

db = 'observatory'

map_features_outpath = os.path.join(folderPath,'study_region','wgs84_epsg4326','map_features')

for filename in os.listdir(map_features_outpath):
    if filename.endswith(".gpkg"): 
        print('\n{}'.format(filename)),
        command = (
                ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
                ' PG:"host={host} port=5432 dbname={db} '
                ' user={user} password = {pwd}" '
                ' {gpkg} '
                ' -lco geometry_name="geom" '
                ' -lco spatial_index="gist" '
                ).format(host = db_host,
                         db = db,
                         user = db_user,
                         pwd = db_pwd,
                         gpkg = filename) 
        sp.call(command, shell=True,cwd=map_features_outpath)
        continue
    else:
        continue

