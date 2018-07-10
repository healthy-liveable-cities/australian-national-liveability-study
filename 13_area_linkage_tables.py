# Script:  area_linkage_tables.py
# Purpose: Create ABS and non-ABS linkage tables using 2016 data sourced from ABS
#
#          Parcel address points are already associated with Meshblock in the parcel_dwellings table
#          Further linkage with the abs_linkage table (actually, a reduced version of the existing mb_dwellings)
#          facilitates aggregation to ABS area units such as SA1, SA2, SA3, SA4.
#
#          The non-ABS linkage table associated points with the suburb and LGA in which they are located, according
#          to ABS sourced spatial features.
#
#          Regarding linkage of addresses with non-ABS structures, while the ABS provides some 
#          correspondence tables between areas, (e.g. SA2 2016 to LGA 2016) this will not be as accurate
#          for our purposes as taking an address point location and evaluating the polygon it intersects.
#          There are pitfalls in this approach (e.g. if a point lies exactly on a boundary), however
#          this is par for the course when generalising unique units into aggregate categories 
#          (ie. points to averages, sums or variances within contiguous areas).
# 
# Author:  Carl Higgs
# Date:    20180710

# Import arcpy module

import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import numpy
import time
import psycopg2 
from progressor import progressor

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *


# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create ABS and non-ABS linkage tables using 2016 data sourced from ABS'

# INPUT PARAMETERS

A_points = parcel_dwellings

parcel_mb_table    = 'parcel_dwellings'
abs_linkage_table  = 'abs_linkage'

suburb_feature = os.path.basename(abs_suburb).strip('.shp').lower()
lga_feature = os.path.basename(abs_lga).strip('.shp').lower()

create_abslinkage_Table     = '''
  DROP TABLE IF EXISTS abs_linkage;
  CREATE TABLE IF NOT EXISTS abs_linkage AS
    SELECT 
      mb_code_2016                    ,
      mb_category_name_2016           ,
      dwelling                        ,
      person                          ,                 
      sa1_7digit                      ,
      sa2_name_2 AS sa2_name_2016     ,
      sa3_name_2 AS sa3_name_2016     ,
      sa4_name_2 AS sa4_name_2016     ,
      gccsa_name                      ,
      state_name                      ,
      area_albers_sqkm                ,
      shape_area/10000 AS area_ha     ,
      geom
    FROM 
      mb_dwellings ;
  ALTER  TABLE abs_linkage ADD PRIMARY KEY (mb_code_2016);
  CREATE INDEX IF NOT EXISTS mb_code_2016_gix ON abs_linkage USING GIST (geom);
  '''

create_non_abslinkage_Table     = '''
  CREATE TABLE IF NOT EXISTS non_abs_linkage AS
    SELECT
      {0},
      ssc_code_2 AS ssc_code_2016,
      ssc_name_2 AS ssc_name_2016,
      lga_code_2 AS lga_code_2016,
      lga_name_2 AS lga_name_2016
      from parcel_dwellings a, 
      {1} b, 
      {2} c 
      where st_contains(b.geom,a.geom) AND st_contains(c.geom,a.geom);
  ALTER  TABLE non_abs_linkage ADD PRIMARY KEY ({0});
  '''.format(points_id,suburb_feature, lga_feature)
  
# OUTPUT PROCESS
task = 'Extract parcel PFI and meshblock code from {}, and create ABS linkage table.'.format(A_points)
print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))
# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()

curs.execute(create_abslinkage_Table)
conn.commit()

print("Copy ABS geometries to postgis...")
# Note that the option flag '-lco precision = NO' is used to mitigate a datatype error; ie. it recasts
# a redundantly double precision field to a more appropriately concise FLOAT8 (which is fine)
# # ERROR 1: COPY statement failed.
# # ERROR:  numeric field overflow
# # DETAIL:  A field with precision 13, scale 11 must round to an absolute value less than 10^2.
# # CONTEXT:  COPY main_ssc_2016_aust, line 5, column area_alber: "127.17000000000"
# ie. instead of recording area as "127.17000000000", it records "127.17" -- better! and works.
# see: https://gis.stackexchange.com/questions/254671/ogr2ogr-error-importing-shapefile-into-postgis-numeric-field-overflow

for area in [abs_SA1,abs_SA2, abs_SA3, abs_SA4, abs_lga, abs_suburb]:
  feature = os.path.basename(area).strip('.shp').lower()
  name = feature.strip('main_')[0:3]
  print('{}: '.format(name)), 
  command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" -a_srs "EPSG:{srid}" '.format(srid = srid) \
          + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
          + 'user={user} password = {pwd}" '.format(user = db_user,pwd = db_pwd) \
          + '{shp} '.format(shp = area) \
          + '-lco geometry_name="geom"  -lco precision=NO ' \
          + '-nlt MULTIPOLYGON'
  sp.call(command, shell=True)

print("Granting privileges to python and arcgis users... "),
curs.execute(grant_query)
conn.commit()
print("Done.")

print("Create non-ABS linkage table (linking point IDs with suburbs and LGAs... "),
curs.execute(create_non_abslinkage_Table)
conn.commit()
print("Done")

# output to completion log    
script_running_log(script, task, start, locale)

# clean up
conn.close()
   