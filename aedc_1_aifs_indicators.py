# Script:  17_aedc_indicators_aifs.py
# Purpose: Create parcel indicators for national liveability project
# Author:  Carl Higgs 
# Date:    20180717

import time
import psycopg2 
import numpy as np
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))
# Indicator configuration sheet is 'df_inds', read in from config file in the config script

# Restrict to indicators associated with study region (except distance to closest dest indicators)
ind_aedc = pandas.read_excel(xls, 'aedc')
ind_aedc = ind_aedc[ind_aedc.apply (lambda r: ('{}'.format(r.table)!='nan') and ('{}'.format(r['AEDC 1 - AIFS linkage'])!='nan'),axis = 1)].copy()

indicators = '\n'.join(ind_aedc.apply(lambda r: '{table}.{variable} AS {aedc_name},'.format(table = r.table, variable = r.variable, aedc_name = r.aedc_name),axis = 1).unique())

joins = '\n'.join(ind_aedc.apply(lambda r: 'LEFT JOIN {table} ON {join}."{join_id}" = {table}."{table_id}"'.format(table = r.table, table_id = r.table_id, join = r.join, join_id = r.join_id),axis = 1).unique())

print("Creating compiled set of parcel level indicators..."),   
# Define parcel level indicator table creation query
# Note that we modify inds slightly later when aggregated to reflect cutoffs etc
sql = '''
DROP TABLE IF EXISTS aedc_indicators_aifs;
CREATE TABLE aedc_indicators_aifs AS
SELECT
parcel_dwellings.{id},
'{full_locale}' AS study_region,
'{locale}' AS locale,
e.exclude,
{indicators}            
aos.aos_jsonb,
parcel_dwellings.geom                   
FROM
parcel_dwellings                                                                                
LEFT JOIN (SELECT {id}, 
                  string_agg(indicator,',') AS exclude 
             FROM excluded_parcels 
             GROUP BY {id}) e 
       ON parcel_dwellings.{id} = e.{id}
{sources}
LEFT JOIN (SELECT {id}, 
                  jsonb_agg((obj - 'aos_id' - 'distance') || jsonb_build_object('aos', obj->'aos_id') || jsonb_build_object('m', obj->'distance') ) AS aos_jsonb
             FROM od_aos_jsonb,
                  jsonb_array_elements(attributes) obj
           GROUP BY {id}) aos
       ON parcel_dwellings.{id} = aos.{id};
CREATE UNIQUE INDEX IF NOT EXISTS aedc_indicators_aifs_idx ON  aedc_indicators_aifs ({id});
'''.format(id = points_id, indicators = indicators, sources = joins,full_locale = full_locale,locale=locale)

curs.execute(sql)
conn.commit()
print(" Done.")

sql = '''
DROP TABLE IF EXISTS study_region_locale;
CREATE TABLE study_region_locale AS
SELECT
'{full_locale}' AS study_region,
'{locale}' AS locale,
sos.*,
included.count
FROM study_region_all_sos sos
LEFT JOIN (SELECT sos_name_2016, 
                  COUNT(a.*),  
                  sum(case 
                        when b.indicator is null 
                        then 1
                        else 0 
                      end) AS include 
             FROM parcel_sos a 
             LEFT JOIN excluded_parcels b 
             USING ({id}) 
             GROUP BY sos_name_2016) included
       ON sos.sos_name_2016 = included.sos_name_2016;
'''.format(id = points_id, full_locale = full_locale, locale = locale)
curs.execute(sql)
conn.commit()
print(" Done.")

sql = '''
ALTER TABLE aos_acara_naplan ADD COLUMN IF NOT EXISTS locale text;
UPDATE aos_acara_naplan SET locale = '{locale}';
'''.format(locale = locale)
curs.execute(sql)
conn.commit()
print(" Done.")

print("Can you please also run the following from the command prompt in the following directory: {folderPath}/study_region//wgs84_epsg4326/".format(folderPath = folderPath))
command = 'pg_dump -U postgres -h localhost -W  -t "study_region_locale" -t "aedc_indicators_aifs" -t "open_space_areas" -t "aos_acara_naplan" {db} > aedc_aifs_{db}.sql'.format(db = db)
sp.call(command, shell=True,cwd=('../data/studyregion'))   

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
