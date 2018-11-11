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
  sos = '''
  CREATE TABLE pos_400m_{ind}_sos AS
  SELECT sos_name_2 AS sos                ,
         SUM(osm_foi_{ind})     AS foi_n  , 
         100*avg(osm_foi_{ind}) AS foi_pct, 
         SUM(osm_osm_{ind})     AS osm_n  , 
         100*avg(osm_osm_{ind}) AS osm_pct, 
         SUM(osm_vpa_{ind})     AS vpa_n  , 
         100*avg(osm_vpa_{ind}) AS vpa_pct, 
         COUNT(*) AS total_n
  FROM pos_400m_{ind} p 
  LEFT JOIN (SELECT s.sos_name_2,
                    s.geom 
               FROM main_sos_2016_aust s, 
                    gccsa_2018 g 
             WHERE ST_Intersects(s.geom,g.geom)) t
  ON ST_Intersects (p.geom,t.geom)
  GROUP BY sos_name_2;
  '''.format(ind = ind)
  curs.execute(sos)
  conn.commit()
  
  ssc = '''
  CREATE TABLE pos_400m_{ind}_ssc AS
  SELECT ssc_name_2 AS ssc                ,
         SUM(osm_foi_{ind})     AS foi_n  , 
         100*avg(osm_foi_{ind}) AS foi_pct, 
         SUM(osm_osm_{ind})     AS osm_n  , 
         100*avg(osm_osm_{ind}) AS osm_pct, 
         SUM(osm_vpa_{ind})     AS vpa_n  , 
         100*avg(osm_vpa_{ind}) AS vpa_pct, 
         COUNT(*) AS total_n
  FROM pos_400m_{ind} p 
  LEFT JOIN (SELECT s.ssc_name_2,
                    s.geom 
               FROM main_ssc_2016_aust s, 
                    gccsa_2018 g 
             WHERE ST_Intersects(s.geom,g.geom)) t
  ON ST_Intersects (p.geom,t.geom)
  GROUP BY ssc_name_2;
  '''.format(ind = ind)
  curs.execute(ssc)
  conn.commit()
  
# output to completion log    
script_running_log(script, task, start, locale)
conn.close()