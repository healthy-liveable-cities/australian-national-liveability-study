# Purpose: Create highlife backup
# Author:  Carl Higgs 
# Date:    2020-03-27

# To restore highlife backup using Postgres with Postgis, run each of the following commands
# noting that you will be prompted for your administrator password after each:
#
# psql -U postgres -c "CREATE DATABASE highlife_perth_2019;"
# psql -U postgres -d highlife_perth_2019 -c "CREATE EXTENSION POSTGIS; CREATE EXTENSION hstore; CREATE SCHEMA ind_point; CREATE SCHEMA d_3200m_cl;"
# pg_restore -U postgres -d highlife_perth_2019 < ../data/highlife_analysis_perth_20200327.sql
#
# psql -U postgres -c "CREATE DATABASE highlife_melbourne_2019;"
# psql -U postgres -d highlife_melbourne_2019 -c "CREATE EXTENSION POSTGIS; CREATE EXTENSION hstore; CREATE SCHEMA ind_point; CREATE SCHEMA d_3200m_cl;"
# pg_restore -U postgres -d highlife_melbourne_2019 < ../data/highlife_analysis_melbourne_20200327.sql
#
# psql -U postgres -c "CREATE DATABASE highlife_sydney_2019;"
# psql -U postgres -d highlife_sydney_2019 -c "CREATE EXTENSION POSTGIS; CREATE EXTENSION hstore; CREATE SCHEMA ind_point; CREATE SCHEMA d_3200m_cl;"
# pg_restore -U postgres -d highlife_sydney_2019 < ../data/highlife_analysis_sydney_20200327.sql

import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine
from datetime import datetime
import getpass

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Export Highlife indicator region estimates'

date = datetime.today().strftime('%Y%m%d')

admin_db   = raw_input("Database: ")    
admin_user = raw_input("Username: ")
admin_pwd = getpass.getpass("Password for user {} on database {}: ".format(admin_user, admin_db))

dfs_parcel = {}
dfs_block = {}
dfs_building = {}
for locale in sys.argv[1:]:
    print('\n{}\n'.format(region.title()))
    db = 'hl_{locale}_2019'.format(locale = locale)
    engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = admin_user,
                                                                 pwd  = admin_pwd,
                                                                 host = db_host,
                                                                 db   = db))

    out_file = '../data/highlife_analysis_{}_{}.sql'.format(locale,date)
    print("Creating sql dump to: {}".format(out_file)),
    command = commands = ( 
         ' pg_dump -U {db_user}   -F c -b                     '
         ' -t public.area_hl_nh_inds_block                       '
         ' -t public.area_hl_nh_inds_building                    '
         ' -t public.area_indicators_block                       '
         ' -t public.area_indicators_building                    '
         ' -t public.area_linkage                                '
         ' -t public.destination_catalog                         '
         ' -t public.distance_to_cbd                             '
         ' -t public.ind_description                             '
         ' -t public.ind_summary                                 '
         ' -t public.ind_summary_hl_inds                         '
         ' -t public.perth_accesspts_edited                      '
         ' -t public.study_region                                '
         ' -t public.study_region_10000m                         '
         ' -t d_3200m_cl.*                                       '
         ' -t ind_point.*                                        '
         ' {db} > {out_file}'
     ).format(db_user = db_user,
                db = db,
                locale = locale,
                out_file=out_file)
    sp.call(command, shell=True)   
    print("Done.")

    print("Copy results to csv...")
    print("  - parcel")
    sql = '''SELECT column_name FROM information_schema.columns WHERE table_schema='ind_point' AND table_name = '{}';'''
    li_cols = pandas.read_sql(sql.format('parcel_indicators'),engine).values
    li_cols = ['p.{}'.format(x[0]) for x in li_cols if x!= 'geom']
    hl_cols = pandas.read_sql(sql.format('parcel_hl_inds_nh'),engine).values
    hl_cols = [x[0] for x in hl_cols if 'p.{}'.format(x[0]) not in li_cols]
    # Copy out parcel level results
    sql = '''
    COPY (SELECT {li},
                 {hl} 
            FROM ind_point.parcel_indicators p 
       LEFT JOIN ind_point.parcel_hl_inds_nh USING ({points_id}))
      TO '../data/highlife_spatial_access_point_{date}.csv' CSV DELIMITER ',' HEADER;
    '''.format(li = ','.join(li_cols),
               hl = ','.join(hl_cols),
               points_id = points_id)
    engine.execute(sql)

    print("  - block")
    sql = '''SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name = '{}';'''
    li_cols = pandas.read_sql(sql.format('area_indicators_block'),engine).values
    li_cols = ['p.{}'.format(x[0]) for x in li_cols if x!= 'geom']
    hl_cols = pandas.read_sql(sql.format('area_hl_nh_inds_block'),engine).values
    hl_cols = [x[0] for x in hl_cols if 'p.{}'.format(x[0]) not in li_cols]
    # Copy out block level results
    sql = '''
    COPY (SELECT {li},
                 {hl} 
            FROM public.area_indicators_block p 
       LEFT JOIN public.area_hl_nh_inds_block USING (blockid))
      TO '../data/highlife_spatial_block_{date}.csv' CSV DELIMITER ',' HEADER;
    '''.format(li = ','.join(li_cols),
               hl = ','.join(hl_cols))
    engine.execute(sql)

    print("  - building")
    sql = '''SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name = '{}';'''
    li_cols = pandas.read_sql(sql.format('area_indicators_building'),engine).values
    li_cols = ['p.{}'.format(x[0]) for x in li_cols if x!= 'geom']
    hl_cols = pandas.read_sql(sql.format('area_hl_nh_inds_building'),engine).values
    hl_cols = [x[0] for x in hl_cols if 'p.{}'.format(x[0]) not in li_cols]
    # Copy out Building level results
    sql = '''
    COPY (SELECT {li},
                 {hl} 
            FROM public.area_indicators_building p 
       LEFT JOIN public.area_hl_nh_inds_building USING (buildlingno))
      TO '../data/highlife_spatial_buiding_{date}.csv' CSV DELIMITER ',' HEADER;
    '''.format(li = ','.join(li_cols),
               hl = ','.join(hl_cols))
    engine.execute(sql)

    # output to completion log
    script_running_log(script, task, start)
    engine.dispose()

