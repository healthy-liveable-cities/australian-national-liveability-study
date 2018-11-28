# Script:  15_od_aos_testing_melb_vpa.py
# Purpose: Calcault distance to nearest AOS within 3.2km, 
#          or if none within 3.2km then distance to closest
#
#          This is a test implementation of the script which facilitates
#          comparisons with OSM and VicMap networks for accessing 
#          POS constructed using VPA and FOI data, or open spaces (OS) using OSM
#          In the case of OSM, a post-processing script narrows down to evaluate 
#          access to the subset of AOS that contain OS meeting definition of POS
# Authors: Carl Higgs, Julianna Rozek

# Julianna Rozek 25 October 2018
#
# Definitions of open space, following the VPA
#Potential measures
# Percentage of dwellings in a suburb within 400m walking distance of any public open space
# Percentage of dwellings in a suburb within 400m walking distance of a public open space >= 1ha, or any size if it contains an outdoor sports facility
# 
# 1 is proposed and used by the VPA in the 'Metropolitan open space network' (2017).
# 2 is is based on the Victorian Planning Provisions Standard c13 (2009).
# 
# While only the public open space measure is formally state government policy, the VPA's open space measure is much more recent and includes lots of justifying documentation. So I think it would be good to measure both.
# 
# The datasets also allow measurement of distance to any open space (including Restricted and Private space), but I'm not sure whether this is of particular interest. 

# Open space
# Includes all open-space related features, including Public, Restricted and Private open space
#  
# Public open space
# Features selected according to VPA's Public open space definition:
# Excludes golf courses, race courses and cemeteries 
# Excludes sports facilities that have exclusively 'highly limited' or 'closed' access (eg. equestrian facilities, lawn bowls)
# Includes outdoor sports facilities 
# Excludes all features within land-uses that may restrict access (eg. schools, hospitals)
# 
# Open space datasets
# VPA
# Link to dataset
# Created 7/12/2016, Data last updated 16/2/2018
# Downloaded 25/10/2018
# Coverage over 32 municipalities in metropolitan Melbourne (does not cover the whole GCCSA; missing Murrindindi Shire, Mitchell Shire, Macedon Ranges Shire, Moorabool Shire)
# Not currently updated.
# Created by the VPA as part of 'Metropolitan open space network' (2017). Uses a range of state and local government sources. Includes open space features pre-categorised into Public, Restricted or Private open space.
# 
# FOI
# Link to dataset
# Link to documentation
# Created 08/03/2016, Updated 01/01/2018, Currency date (?not sure what this is) 13/10/2018
# Downloaded 16/10/2018
# Coverage over the whole of Victoria.
# Dataset updated annually subject to funding being available.
# Created by DELWP as part of Vicmap's Features of Interest dataset. Uses data from a range of federal, state and local government departments, authorities and agencies. Includes a range of cultural/infrastructure features.
# 
# OSM
# Downloaded 01/10/2018
# Coverage worldwide.
# No set data maintenance schedule.
# Created by the community using open source data and manual additions.
# 
# Open space data processing
# VPA and FOI datasets were processed in ArcMap 10.6.0.8321.
# Each individual dataset includes all open space. Public open space features have been dissolved, and Restricted or Private features have been dissolved.
# 
# The final datasets have two binary tags:
# sports
# 0= feature does not intersect a sports facility
# 1= feature intersects a sports facility
# public
# 0= feature is not a public open space (ie. is Restricted or Private)
# 1= feature is a public open space
# 
# VPA
# Features where OS_TYPE = 'Public open space' selected, dissolved, and tagged public=1. Where these dissolved features intersect OS_CATEGOR = 'Sportsfields and organised recreation' these are tagged sports=1.
# 
# Features where OS_TYPE = 'Restricted public land' OR 'Private open space' selected, dissolved and tagged public=0. Where these dissolved features intersect OS_CATEGOR = 'Sportsfields and organised recreation' these are tagged sports=1.
# 
# FOI
# Features where FEATSUBTYP are public (see table below) selected. Those within exclusion features (see table below) tagged as public=0. Remaining features dissolved and tagged public=1. Where dissolved features intersect FTYPE = 'sport facility' these are tagged sports=1.
# 
# Features where FEATSUBTYP are not public (see table) selected, dissolved and tagged public =0. Where these dissolved features intersect FTYPE = 'sports facility' these are tagged sports=1.
################################################################################
# This script
#  - assumes script 15b has been run
#  - makes use of ABS features which are actually imported in script 16 -- in particular, suburbs (ssc) and sections of state (sos)
#
# Additional source tables for summarising results were created directly using the following sql
#   
# DROP TABLE IF EXISTS study_region_all_sos;
# CREATE TABLE study_region_all_sos AS 
# SELECT b.sos_name_2 AS sos_name_2016, 
#        ST_Intersection(a.geom, b.geom) AS geom
# FROM 
# gccsa_2018 a, 
# main_sos_2016_aust b 
# WHERE ST_Intersects(a.geom,b.geom);
# CREATE INDEX IF NOT EXISTS idx_sos      ON study_region_all_sos (sos_name_2016);
# CREATE INDEX IF NOT EXISTS idx_sos_geom ON study_region_all_sos USING gist(geom);
#   
# DROP TABLE IF EXISTS study_region_ssc;
# CREATE TABLE study_region_ssc AS 
# SELECT b.ssc_name_2 AS ssc_name_2016, 
#        b.geom
# FROM 
# gccsa_2018 a, 
# main_ssc_2016_aust b 
# WHERE ST_Intersects(a.geom,b.geom);  
# CREATE INDEX IF NOT EXISTS idx_ssc      ON study_region_ssc (ssc_name_2016);
# CREATE INDEX IF NOT EXISTS idx_ssc_geom ON study_region_ssc USING gist(geom);

import arcpy, arcinfo
import os
import time
import sys
import psycopg2 

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Compile a table comparing indicator results using various network and pos source combinations'

# initiate postgresql connection
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

print("Creating comparison tables, if not already existing...")
for ind in ['any','gr1ha','gr1ha_sp']:
  print("Tables for {} pos in 400m...".format(ind))
  print("  - Create indices for id and geom on indicator table... "),
  create_indices = '''
  CREATE INDEX IF NOT EXISTS idx_pos_400m_{ind} ON pos_400m_{ind} ({id});
  CREATE INDEX IF NOT EXISTS idx_pos_400m_{ind}_geom ON pos_400m_{ind} USING gist(geom);
  '''.format(ind = ind, id = points_id.lower())
  # print(create_indices)
  curs.execute(create_indices)
  conn.commit()
  print("Done.")
  
  print("  - Sections of State (SOS)... "),
  sos = '''
  DROP TABLE IF EXISTS pos_400m_{ind}_sos;
  CREATE TABLE pos_400m_{ind}_sos AS
  SELECT s.sos_name_2016 AS sos                 ,
         SUM(osm_foi)     AS osm_foi_n          , 
         100*avg(osm_foi) AS osm_foi_pct        , 
         SUM(osm_osm)     AS osm_osm_n          , 
         100*avg(osm_osm) AS osm_osm_pct        , 
         SUM(osm_vpa)     AS osm_vpa_n          , 
         100*avg(osm_vpa) AS osm_vpa_pct        , 
         SUM(osm_osm2)     AS osm_osm2_n        , 
         100*avg(osm_osm2) AS osm_osm2_pct      , 
         SUM(vicmap_foi)     AS vicmap_foi_n    , 
         100*avg(vicmap_foi) AS vicmap_foi_pct  , 
         SUM(vicmap_osm)     AS vicmap_osm_n    , 
         100*avg(vicmap_osm) AS vicmap_osm_pct  , 
         SUM(vicmap_vpa)     AS vicmap_vpa_n    , 
         100*avg(vicmap_vpa) AS vicmap_vpa_pct  , 
         SUM(vicmap_osm2)     AS vicmap_osm2_n  , 
         100*avg(vicmap_osm2) AS vicmap_osm2_pct, 
         COUNT(*) AS total_n                    ,
         s.geom AS geom         
  FROM pos_400m_{ind} p 
  LEFT JOIN parcel_sos t
  ON p.{id} = t.{id}
  LEFT JOIN study_region_all_sos s
  ON t.sos_name_2016 = s.sos_name_2016
  GROUP BY s.sos_name_2016, s.geom;
  '''.format(ind = ind, id = points_id.lower())
  print(sos)
  curs.execute(sos)
  conn.commit()
  print("Done.")
  
  print("  - Suburbs (SSC)... "),
  ssc = '''
  DROP TABLE IF EXISTS pos_400m_{ind}_ssc;
  CREATE TABLE pos_400m_{ind}_ssc AS
  SELECT s.ssc_name_2016 AS ssc                 ,
         SUM(osm_foi)     AS osm_foi_n          , 
         100*avg(osm_foi) AS osm_foi_pct        , 
         SUM(osm_osm)     AS osm_osm_n          , 
         100*avg(osm_osm) AS osm_osm_pct        , 
         SUM(osm_vpa)     AS osm_vpa_n          , 
         100*avg(osm_vpa) AS osm_vpa_pct        , 
         SUM(osm_osm2)     AS osm_osm2_n        , 
         100*avg(osm_osm2) AS osm_osm2_pct      , 
         SUM(vicmap_foi)     AS vicmap_foi_n    , 
         100*avg(vicmap_foi) AS vicmap_foi_pct  , 
         SUM(vicmap_osm)     AS vicmap_osm_n    , 
         100*avg(vicmap_osm) AS vicmap_osm_pct  , 
         SUM(vicmap_vpa)     AS vicmap_vpa_n    , 
         100*avg(vicmap_vpa) AS vicmap_vpa_pct  , 
         SUM(vicmap_osm2)     AS vicmap_osm2_n  , 
         100*avg(vicmap_osm2) AS vicmap_osm2_pct, 
         COUNT(*) AS total_n                    ,
         s.geom AS geom
  FROM pos_400m_{ind} p 
  LEFT JOIN non_abs_linkage t
  ON p.{id} = t.{id}
  LEFT JOIN study_region_ssc s
  ON t.ssc_name_2016 = s.ssc_name_2016
  GROUP BY s.ssc_name_2016, s.geom;
  '''.format(ind = ind, id = points_id.lower())
  print(ssc)
  curs.execute(ssc)
  conn.commit()
  print("Done.")
  
road_inds = '''  
CREATE TABLE IF NOT EXISTS osm_sos_summary AS
SELECT sos_name_2016,
       SUM(ST_Length(ST_Intersection(e.geom,s.geom))),
       s.geom
FROM edges e, study_region_all_sos s
WHERE ST_Intersects(e.geom,s.geom)
GROUP BY s.sos_name_2016,s.geom;

CREATE TABLE IF NOT EXISTS osm_ssc_summary AS
SELECT ssc_name_2016,
       SUM(ST_Length(ST_Intersection(e.geom,s.geom))) ,
       s.geom
FROM edges e, study_region_ssc s
WHERE ST_Intersects(e.geom,s.geom)
GROUP BY s.ssc_name_2016,s.geom;

CREATE TABLE IF NOT EXISTS vicmap_sos_summary AS
SELECT sos_name_2016,
       SUM(ST_Length(ST_Intersection(e.geom,s.geom))),
       s.geom
FROM edges_vicmap e, study_region_all_sos s
WHERE ST_Intersects(e.geom,s.geom)
GROUP BY s.sos_name_2016,s.geom;

CREATE TABLE IF NOT EXISTS vicmap_ssc_summary AS
SELECT ssc_name_2016,
       SUM(ST_Length(ST_Intersection(e.geom,s.geom))) ,
       s.geom
FROM edges_vicmap e, study_region_ssc s
WHERE ST_Intersects(e.geom,s.geom)
GROUP BY s.ssc_name_2016,s.geom;
'''
curs.execute(road_inds)
conn.commit()
  
# output to completion log    
script_running_log(script, task, start, locale)
conn.close()