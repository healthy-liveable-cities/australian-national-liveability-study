# Script:  19_area_indicators.py
# Purpose: Create area level indicator tables
# Author:  Carl Higgs 
# Date:    20 July 2018

import os
import sys
import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create area level indicator tables for Disability and Health ({})'.format(locale)
print(task)
# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db))

sql = '''
SELECT
sa1_maincode_2016                            ,
study_region                                 ,
locale                                       ,
dwelling                                     ,
person                                       ,
sample_count                                 ,
sample_count_per_ha                          ,
area_ha                                      ,
food_02                                      ,
food_04                                      ,
food_10                                      ,
food_13                                      ,
food_15                                      ,
food_01                                      ,
food_04                                      ,
food_12                                      ,
food_15                                      ,
food_23_soft                                 ,
food_23_hard                                 ,
food_06                                      ,
food_07                                      ,
food_08                                      ,
food_08                                      ,
food_09                                      ,
food_17                                      ,
food_18                                      ,
food_19                                      ,
food_20                                      ,
food_07                                      ,
food_06                                      ,
food_10                                      ,
food_11                                      ,
food_23_soft                                 ,
food_23_hard                                 ,
alc_01                                       ,
alc_02                                       ,
dist_m_alcohol_offlicence                    ,
dist_m_alcohol_onlicence                     ,
dist_m_alcohol_osm                           ,
dist_m_bar_osm                               ,
childcare_01_soft                            ,
childcare_01_hard                            ,
childcare_02_hard                            ,
childcare_02_soft                            ,
dist_m_libraries_2018                        ,
dist_m_nhsd_2017_gp                          ,
dist_m_nhsd_2017_aged_care                   ,
dist_m_nhsd_2017_aged_care_residential       ,
dist_m_nhsd_2017_childdev_early_support      ,
dist_m_nhsd_2017_childdev_play_programs      ,
dist_m_nhsd_2017_childdev_playgroup          ,
dist_m_nhsd_2017_childdev_support            ,
dist_m_nhsd_2017_childprot_familyservices    ,
dist_m_nhsd_2017_immunisation                ,
dist_m_nhsd_2017_mc_family_health            ,
dist_m_nhsd_2017_pharmacy                    ,
dist_m_nhsd_2017_family_counselling          ,
dist_m_nhsd_2017_general_counselling         ,
dist_m_nhsd_2017_dentist                     ,
dist_m_nhsd_2017_childhood_intervention      ,
dist_m_nhsd_2017_mental_health_adult         ,
dist_m_nhsd_2017_mental_health_child         ,
dist_m_nhsd_2017_mental_health_general       ,
dist_m_nhsd_2017_mental_health_psychology    ,
dist_m_nhsd_2017_physical_activity_recreation,
dist_m_nhsd_2017_pediatrics                  ,
dist_m_nhsd_2017_hospital                    ,
dist_m_activity_centres_2017                 ,
walk_20_soft                                 ,
walk_20_hard                                 ,
walk_19                                      ,
walk_18                                      ,
walk_20_soft                                 ,
walk_20_hard                                 ,
NULL::double precision AS walk_national      ,
os_public_01_soft                            ,
os_public_01_hard                            ,
os_public_02_soft                            ,
os_public_02_hard                            ,
trans_06_soft                                ,
trans_06_hard                                ,
FROM li_inds_sa1_dwelling;  
'''.format(locale)

df = pandas.read_sql_query(sql,con=engine)
df.to_csv('D:/ntnl_li_2018_template/data/dh_inds_{}_sa1_dwellings_2018_{}.csv'.format(locale,time.strftime('%Y%d%m')),index=False)

if locale!='australia':
    sql = '''
    ALTER TABLE open_space_areas ADD COLUMN IF NOT EXISTS locale text;
    UPDATE open_space_areas SET locale = '{locale}';
    '''.format(locale = locale)
    curs.execute(sql)
    conn.commit()

out_dir = 'D:/ntnl_li_2018_template/data/'
out_file = 'ntnl_li_inds_{}_{}_Fc.sql'.format(locale,year)
print("Creating sql dump to: {}".format(os.path.join(out_dir,out_file))),
command = 'pg_dump -U {db_user} -h localhost -Fc -t "parcel_indicators" -t "dest_closest_indicators" -t "dest_array_indicators" -t "od_aos_jsonb" -t "open_space_areas" -t "ind_summary" -t "exclusion_summary" -t "area_indicators_mb_json" -t "area_linkage" -t "li_inds_sa1_dwelling" {db} > {out_file}'.format(db = db,db_user = db_user,out_file=out_file)    
sp.call(command, shell=True,cwd=out_dir)   
print("Done.")

# output to completion log    
script_running_log(script, task, start, locale)

