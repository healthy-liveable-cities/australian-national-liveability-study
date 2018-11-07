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
print("  - for any POS within 400m, "),
pos_400m_any = '''
CREATE TABLE IF NOT EXISTS pos_400m_any AS 
SELECT {id},
0::int AS osm_foi_any     ,
0::int AS osm_osm_any     ,
0::int AS osm_vpa_any     ,
0::int AS vicmap_foi_any  ,
0::int AS vicmap_osm_any  ,
0::int AS vicmap_vpa_any  ,
geom
FROM parcel_dwellings;
CREATE INDEX IF NOT EXISTS idx_pos_400m_comparison ON pos_400m_any ({id});
'''.format(id = points_id.lower())
curs.execute(pos_400m_any)
conn.commit()
print("Done.")
print("  - for POS >= 1 Ha within 400m, "),
pos_400m_gr1ha = '''
CREATE TABLE IF NOT EXISTS pos_400m_gr1ha AS 
SELECT {id},
0::int AS osm_foi_gr1ha   ,
0::int AS osm_osm_gr1ha   ,
0::int AS osm_vpa_gr1ha   ,
0::int AS vicmap_foi_gr1ha,
0::int AS vicmap_osm_gr1ha,
0::int AS vicmap_vpa_gr1ha,
geom
FROM parcel_dwellings;
CREATE INDEX IF NOT EXISTS idx_pos_400m_gr1ha ON pos_400m_gr1ha ({id});
'''.format(id = points_id.lower())
curs.execute(pos_400m_gr1ha)
conn.commit()
print("Done.")

print("  - for POS >= 1 Ha or with a sport within 400m, "),
pos_400m_gr1ha_sp = '''
CREATE TABLE IF NOT EXISTS pos_400m_gr1ha_sp AS 
SELECT {id},
0::int AS osm_foi_gr1ha_sp   ,
0::int AS osm_osm_gr1ha_sp   ,
0::int AS osm_vpa_gr1ha_sp   ,
0::int AS vicmap_foi_gr1ha_sp,
0::int AS vicmap_osm_gr1ha_sp,
0::int AS vicmap_vpa_gr1ha_sp,
geom
FROM parcel_dwellings;
CREATE INDEX IF NOT EXISTS idx_pos_400m_gr1ha_sp ON pos_400m_gr1ha_sp ({id});
'''.format(id = points_id.lower())
curs.execute(pos_400m_gr1ha_sp)
conn.commit()
print("Done.")


os_dict = {"osm":"open_space_areas",
           "foi":"melb_foi_20181030",
           "vpa":"melb_vpa_20181025"}

public_dict = {"osm":"open_space_areas",
           "foi":"melb_foi_20181030",
           "vpa":"melb_vpa_20181025"}           

for network in ['vicmap','osm']:
  for pos in ['foi','osm','vpa']:
    table = 'od_aos_{network}_{pos}_jsonb'.format(network = network,pos = pos)
    print('\nProcessing indicators for {network} network routing to {pos} public open space...'.format(network = network,pos = pos))
    check_table_exists = '''
    SELECT EXISTS (
    SELECT 1
    FROM   information_schema.tables 
    WHERE  table_schema = 'public'
    AND    table_name = '{table}'
    );
    '''.format(table = table)
    # print(check_table_exists)
    curs.execute(check_table_exists)
    table_exists = list(curs)[0][0]
    print("  - JSON results table {table} exists? {answer}".format(table = table, answer = table_exists))
    
    if table_exists:
      if pos in ['foi','vpa']:
        print("    - updating access indicator to any POS within 400m ({network}_{pos}_any)... ".format(network = network,pos = pos)),
        any_update = '''
        UPDATE pos_400m_any ind
           SET {network}_{pos}_any = 1
        WHERE EXISTS 
        (SELECT 1 
         FROM (SELECT {id},
                     (obj->>'aos_id')::int AS aos_id
               FROM {table}, 
                    jsonb_array_elements(attributes) obj
               WHERE obj->'distance'<'400' ) o
        LEFT JOIN {os_source} pos  
               ON o.aos_id = pos.aos_id
            WHERE pos.public = 1
              AND pos.aos_id IS NOT NULL
              AND ind.{id} = o.{id});
              '''.format(id = points_id.lower(),table = table,network = network,pos = pos,os_source = os_dict[pos])
        # print(any_update)
        curs.execute(any_update)
        print("Done")
        print("    - updating access indicator for POS >= 1 Ha within 400m ({network}_{pos}_gr1ha)... ".format(network = network,pos = pos)),
        large_update = '''
        UPDATE pos_400m_gr1ha ind
           SET {network}_{pos}_gr1ha = 1
        WHERE EXISTS 
        (SELECT 1 
         FROM (SELECT {id},
                     (obj->>'aos_id')::int AS aos_id
               FROM {table}, 
                    jsonb_array_elements(attributes) obj
               WHERE obj->'distance'<'400' ) o
        LEFT JOIN {os_source} pos  
               ON o.aos_id = pos.aos_id
            WHERE public = 1
              AND (pos.area_ha >= 1)
              AND pos.aos_id IS NOT NULL
              AND ind.{id} = o.{id});
              '''.format(id = points_id.lower(),table = table,network = network,pos = pos,os_source = os_dict[pos])
        curs.execute(large_update)
        print("Done")
        print("    - updating access indicator for POS >= 1 Ha or a sport within 400m ({network}_{pos}_gr1ha)... ".format(network = network,pos = pos)),
        large_sport_update = '''
        UPDATE pos_400m_gr1ha_sp ind
           SET {network}_{pos}_gr1ha_sp = 1
        WHERE EXISTS 
        (SELECT 1 
         FROM (SELECT {id},
                     (obj->>'aos_id')::int AS aos_id
               FROM {table}, 
                    jsonb_array_elements(attributes) obj
               WHERE obj->'distance'<'400' ) o
        LEFT JOIN {os_source} pos  
               ON o.aos_id = pos.aos_id
            WHERE public = 1
              AND (pos.area_ha >= 1
                   OR
                   sports = 1)
              AND pos.aos_id IS NOT NULL
              AND ind.{id} = o.{id});
              '''.format(id = points_id.lower(),table = table,network = network,pos = pos,os_source = os_dict[pos])
        curs.execute(large_sport_update)
        print("Done")
      if pos == 'osm':
        print("    - updating access indicator to any POS within 400m ({network}_{pos}_any)... ".format(network = network,pos = pos)),
        any_update = '''
        UPDATE pos_400m_any ind
           SET {network}_{pos}_any = 1
        WHERE EXISTS 
        (SELECT 1 
         FROM (SELECT {id},
                     (obj->>'aos_id')::int AS aos_id
               FROM {table}, 
                    jsonb_array_elements(attributes) obj
               WHERE obj->'distance'<'400' ) o
        LEFT JOIN {os_source} pos  
               ON o.aos_id = pos.aos_id
            WHERE pos.aos_id IS NOT NULL
              AND ind.{id} = o.{id});
              '''.format(id = points_id.lower(),table = table,network = network,pos = pos,os_source = os_dict[pos])
        #curs.execute(any_update)
        print("Done")
        print("    - updating access indicator for POS >= 1 Ha within 400m ({network}_{pos}_gr1ha)... ".format(network = network,pos = pos)),
        large_update = '''
        UPDATE pos_400m_gr1ha ind
           SET {network}_{pos}_gr1ha = 1
        WHERE EXISTS 
        (SELECT 1 
         FROM (SELECT {id},
                     (obj->>'aos_id')::int AS aos_id
               FROM {table}, 
                    jsonb_array_elements(attributes) obj
               WHERE obj->'distance'<'400' ) o
        LEFT JOIN public_open_space_areas pos  
               ON o.aos_id = pos.aos_id
            WHERE pos.aos_id IS NOT NULL
              AND pos.aos_ha >= 1
              AND ind.{id} = o.{id});
              '''.format(id = points_id.lower(),table = table,network = network,pos = pos)
        #curs.execute(large_update)
        print("Done")
        print("    - updating access indicator for POS >= 1 Ha or with a sport within 400m ({network}_{pos}_gr1ha_sp)... ".format(network = network,pos = pos))
        large_sport_update = '''
        UPDATE pos_400m_gr1ha_sp ind
           SET {network}_{pos}_gr1ha_sp = 1
        WHERE EXISTS 
        (SELECT 1 
         FROM (SELECT {id},
                     (obj->>'aos_id')::int AS aos_id
               FROM {table}, 
                    jsonb_array_elements(attributes) obj
               WHERE obj->'distance'<'400' ) o
        LEFT JOIN (SELECT aos_id
                   FROM open_space_areas osa,
                        jsonb_array_elements(attributes) obj
                   WHERE obj->'public_access' = 'true'
                   AND  (aos_ha >= 1
                         OR
                         obj->'sport' IS NOT NULL)
                   GROUP BY aos_id) pos
               ON o.aos_id = pos.aos_id
            WHERE pos.aos_id IS NOT NULL
              AND ind.{id} = o.{id});
              '''.format(id = points_id.lower(),table = table,network = network,pos = pos)
        curs.execute(large_sport_update)
        print("Done")    

  
# output to completion log    
script_running_log(script, task, start, locale)
conn.close()