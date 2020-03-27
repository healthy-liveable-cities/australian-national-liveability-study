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

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                             pwd  = db_pwd,
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

# output to completion log
script_running_log(script, task, start)
engine.dispose()

