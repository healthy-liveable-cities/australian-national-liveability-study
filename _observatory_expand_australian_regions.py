# Purpose: Given a database containing Australian Urban Observatory tables for Australia, 
#           expand these as materialized views for seperate study regions
# Author:  Carl Higgs 
# Date:    22 April 2020

import os
import sys
import time
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine
from datetime import datetime

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create area level indicator tables for {}'.format(locale)

today = datetime.today().strftime('%Y-%m-%d')

if len(sys.argv) >= 3:
  db = sys.argv[2]
else:
  exit_advice = '''
  Please specify a database containing the following tables, representing
  an sql dump of Australian data for the Australian Urban Observatory:
  
  boundaries_lga_australia_2018
  boundaries_region_australia_2018
  boundaries_sa1_australia_2018
  boundaries_sos_australia_2018
  boundaries_ssc_australia_2018
  ind_observatory_australia_2018
  observatory_map_lga_australia_2018
  observatory_map_region_australia_2018
  observatory_map_sa1_australia_2018
  observatory_map_ssc_australia_2018
  
  This is generated as a result of running the python script _observatory_dump.py like
  
  python _observatory_dump.py australia
  
  and then recreating this database, like running the following in psql:
  
  CREATE DATABASE obs_source;
  CREATE EXTENSION postgis;
  
  and then restoring the dump:
  psql obs_source < D:ntnl_li_2018/data/observatory/li_map_li_australia_2018.sql postgres
  
  If you had run the above, then you could execute this code with:
  python _observatory_expand_australian_regions.py australia obs_source
  
  By default, previously created materialized views are dropped if they exist (with cascade); 
  however, to negate this effect you can add the command nodrop to the command line
  python _observatory_expand_australian_regions.py australia obs_source nodrop
  
  Futher, if the export of this database to sql is required you can run the command with key word 'export'
  
  python _observatory_expand_australian_regions.py australia obs_source nodrop export
  
  which will output a dated sql file like auo_2020-04-24.sql (ie date in yyyy-mm-dd format)
  
   This file can be restored using pg_restore targetting a database created with PostGIS extension, like so:
  pg_restore -U postgres -Fc -d obs_test < ../data/observatory/2020-04-24/auo_2020-04-24.sql
  
  Good luck!
  '''
  sys.exit(exit_advice)

if 'nodrop' in sys.argv:
    drop = '--'
else:
    drop = ''

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

# This script is specifically concerned with 2018 time point at this stage
year = 2018

# look up table of study regions to locales
locales = {'Adelaide'                   :'adelaide',
           'Albury - Wodonga'           :'albury_wodonga',
           'Ballarat'                   :'ballarat',
           'Bendigo'                    :'bendigo',
           'Brisbane'                   :'brisbane',
           'Cairns'                     :'cairns',
           'Canberra'                   :'canberra',
           'Darwin'                     :'darwin',
           'Geelong'                    :'geelong',
           'Gold Coast - Tweed Heads'   :'goldcoast_tweedheads',
           'Hobart'                     :'hobart',
           'Launceston'                 :'launceston',
           'Mackay'                     :'mackay',
           'Melbourne'                  :'melbourne',
           'Newcastle - Maitland'       :'newcastle_maitland',
           'Perth'                      :'perth',
           'Sunshine Coast'             :'sunshine_coast',
           'Sydney'                     :'sydney',
           'Toowoomba'                  :'toowoomba',
           'Townsville'                 :'townsville',
           'Wollongong '                :'wollongong'}

# Table stubs and key details
# Note: boundaries_sos is not used in AUO, so commented out
tables = {'boundaries_lga'        :{'key':'lga_name_2016'    },
          'boundaries_region'     :{'key':'study_region'     },
          'boundaries_sa1'        :{'key':'sa1_maincode_2016'},
          'boundaries_sos'        :{'key':'sos_name_2016'    },
          'boundaries_ssc'        :{'key':'ssc_name_2016'    },
          'observatory_map_lga'   :{'key':'lga'    },
          'observatory_map_region':{'key':'study_region'     },
          'observatory_map_sa1'   :{'key':'sa1'},
          'observatory_map_ssc'   :{'key':'suburb'    },
          }

if 'nocheck' not in sys.argv:
    print("Ensure source geometries are valid and tables have spatial indices... ")
    for table in [t for t in sorted(tables.keys())]:
        table = '{}_australia_{}'.format(table,year)
        print('  - {}'.format(table))
        sql = '''
        -- There was a self-intersection in the SOS table which caused issues forming regions;
        -- to ensure all works fine, make sure all geoms are valid
        UPDATE {table} 
           SET geom = ST_Multi(ST_CollectionExtract(ST_MakeValid(geom),3))
         WHERE ST_IsValid(geom) = false;
        CREATE INDEX IF NOT EXISTS {table}_gix ON {table} USING GIST (geom);
        '''.format(table = table)
        engine.execute(sql)
    print("Done.")

# loop over study regions to expand out tables from the concise national set
for study_region in locales:
    print(study_region)
    locale = locales[study_region]
    for table in  [t for t in sorted(tables.keys()) if t.startswith('observatory')]:
        in_table = '{}_australia_{}'.format(table,year)
        out_table = '{}_{}_{}'.format(table,locale,year).replace('observatory','li')
        key = tables[table]['key']
        print('  - '+out_table)
        # create observatory area views for study region
        sql = '''
        {drop} DROP TABLE IF EXISTS {out_table};
        CREATE TABLE IF NOT EXISTS  {out_table} AS
        SELECT * 
        FROM {in_table}
        WHERE study_region = '{study_region}';
        CREATE INDEX IF NOT EXISTS {out_table}_idx ON {out_table} ({key});
        CREATE INDEX IF NOT EXISTS {out_table}_gix ON {out_table} USING GIST (geom);
        '''.format(drop = drop,
                   out_table = out_table,
                   in_table = in_table,
                   study_region = study_region,
                   key=key)
        # print(sql)
        engine.execute(sql)
    for table in  [t for t in sorted(tables.keys()) if t.startswith('boundaries') and t not in ['boundaries_region','boundaries_sos']]:
        in_table = '{}_australia_{}'.format(table,year)
        out_table = '{}_{}_{}'.format(table,locale,year)
        key = tables[table]['key']
        print('  - '+out_table)
        # create boundaries for study region
        key_table = '{}_australia_{}'.format(table.replace('boundaries','observatory_map'),year)
        alt_key = tables[table.replace('boundaries','observatory_map')]['key']
        sql = '''
        {drop} DROP TABLE IF EXISTS {out_table};
        CREATE TABLE IF NOT EXISTS  {out_table} AS
        SELECT a.* 
        FROM {in_table} a
        LEFT JOIN {key_table} k ON a.{key} = k.{alt_key}
        WHERE k.study_region = '{study_region}';
        CREATE INDEX IF NOT EXISTS {out_table}_idx ON {out_table} ({key});
        CREATE INDEX IF NOT EXISTS {out_table}_gix ON {out_table} USING GIST (geom);
        '''.format(drop = drop,
                   out_table = out_table,
                   in_table = in_table,
                   study_region = study_region,
                   key=key,
                   key_table=key_table,
                   alt_key=alt_key)
        # print(sql)
        engine.execute(sql)
    for table in ['boundaries_region']:
        out_table = '{}_{}_{}'.format(table,locale,year)
        key = tables[table]['key']
        print('  - '+out_table)
        # create region boundary as union of SA1 boundaries
        geom_table =  'boundaries_sa1_{}_{}'.format(locale,year)
        geom_key = tables['boundaries_sa1']['key']
        sql = '''
        {drop} DROP TABLE IF EXISTS {out_table};
        CREATE TABLE IF NOT EXISTS  {out_table} AS
        SELECT '{study_region}'::text study_region,
               -- The union of SA1 regions approximates the study region for the city,
               -- gaps may exist, and this deals with that
               ST_Union(geom) geom
        FROM (SELECT ST_SetSrid( 
                       ST_MakePolygon (
                          ST_ExteriorRing(
                              (ST_Dump(
                                 ST_Union(geom))).geom)),4326) geom
               FROM {geom_table}) t;
        CREATE INDEX IF NOT EXISTS {out_table}_gix ON {out_table} USING GIST (geom);
        '''.format(drop = drop,
                   out_table = out_table,
                   geom_table = geom_table,
                   study_region = study_region,
                   key=key,
                   alt_key=alt_key)
        engine.execute(sql)
    # for table in ['boundaries_sos']:
        # in_table = '{}_australia_{}'.format(table,year)
        # out_table = '{}_{}_{}'.format(table,locale,year)
        # key = tables[table]['key']
        # print('  - '+out_table)
        # # Create sections of state view as intersection of SOS geom within study region
        # geom_table =  'boundaries_region_{}_{}'.format(locale,year)
        # sql = '''
        # {drop} DROP TABLE IF EXISTS {out_table};
        # SELECT a.sos_name_2016,
           # CASE 
               # WHEN ST_CoveredBy(a.geom, s.geom) 
               # THEN a.geom 
               # ELSE 
                # ST_Multi(
                  # ST_Intersection(a.geom,s.geom)
                  # ) 
               # END AS geom 
        # FROM {in_table} a
        # INNER JOIN {geom_table} s 
                # ON (ST_Intersects(a.geom, s.geom) AND NOT ST_Touches(a.geom, s.geom) );
        # CREATE INDEX IF NOT EXISTS  {out_table}_gix ON {out_table} USING GIST (geom);
        # '''.format(drop = drop,
                   # in_table = in_table,
                   # out_table = out_table,
                   # geom_table = geom_table,
                   # study_region = study_region)
        # print(sql)
        # engine.execute(sql)
            
if 'export' in sys.argv:
    print("\nOutput sql dump for AUO... ")
    command = (
               'pg_dump -Fc -Z 9 postgresql://{user}:{pwd}@{host}:5432/{db} '
               '> '
               '../data/observatory/{today}/auo_{today}.sql'
               ).format(user = db_user,
                        pwd = db_pwd,
                        host = db_host,
                        db = db,
                        today=today)
    sp.call(command, shell=True)
    print('''
    AUO dump exported to ../data/observatory/{today}/auo_{today}.sql
    
    This file can be restored using pg_restore targetting a database created with PostGIS extension, like so:
    pg_restore -U postgres -Fc -d obs_source < ../data/observatory/2020-04-24/auo_2020-04-24.sql
    '''.format(today=today))