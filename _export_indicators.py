# Script:  17_aedc_indicators_aifs.py
# Purpose: Create aedc indicators for AIFS (condensed form)
# Author:  Carl Higgs 
# Date:    20180717

import time
import psycopg2 
import numpy as np
import pandas
import os
import sys
from sqlalchemy import create_engine
import subprocess as sp

date_time = time.strftime("%Y%m%d-%H%M")

# Load settings from ind_study_region_matrix.xlsx
xls = pandas.ExcelFile(os.path.join(sys.path[0],'ind_study_region_matrix.xlsx'))
df_about = pandas.read_excel(xls, 'about')
print(df_about.iloc[0].reset_index()['index'].to_string(index=False).encode('utf'))
df_parameters = pandas.read_excel(xls, 'parameters',index_col=0)

df_parameters.value = df_parameters.value.fillna('')
for var in [x for x in df_parameters.index.values]:
    globals()[var] = df_parameters.loc[var]['value']    

df_studyregion = pandas.read_excel(xls, 'study_regions',index_col=1)
responsible = df_studyregion['responsible']

if len(sys.argv) < 2:
    sys.exit('''
This script requires an argument representing either an analyst, a single locale or a space-delimited set of locales.
For example:
   python _export_indicators.py Carl
   python _export_indicators.py albury_wodonga
   python _export_indicators.py albury_wodonga ballarat cairns launceston newcastle_maitland perth adelaide
''')

who = sys.argv[1]
if who in set(responsible.values):
    locales = responsible[responsible == who].sort_values().index.values.tolist()
elif who in responsible.sort_values().index.values:
    locales = sys.argv[1:]
    who = '_'.join(locales)
else:
    sys.exit('''
    The supplied command argument '{}' does not appear to correspond to either an analyst, a locale or list of locales.  Please check and try again.
    '''.format(who))

out_dir = os.path.join(folderPath,'study_region','ntnl_li_inds')
if not os.path.exists(out_dir):
        os.makedirs(out_dir)
os.environ['PGPASSWORD'] = db_pwd

for locale in locales:
    full_locale = df_studyregion.loc[locale]['full_locale'].encode('utf')
    print('\n{}'.format(full_locale))
    
    start = time.time()
    script = os.path.basename(sys.argv[0])
    task = 'Create aedc indicators for AIFS (condensed form)'
    # Connect to postgresql database     
    db = 'li_{}_{}'.format(locale,year)
    conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
    curs = conn.cursor()
    engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                pwd  = db_pwd,
                                                                host = db_host,
                                                                db   = db))
    sql = '''
    ALTER TABLE od_aos ADD COLUMN IF NOT EXISTS locale text;
    UPDATE od_aos SET locale = '{locale}';
    '''.format(locale = locale)
    curs.execute(sql)
    conn.commit()
    out_file = 'ntnl_li_inds_{}_{}_Fc.sql'.format(db,time.strftime("%Y%m%d-%H%M"))
    print("\tCreating sql dump to: {}".format(os.path.join(out_dir,out_file))),
    command = 'pg_dump -U {db_user} -h localhost -Fc -t "parcel_indicators" -t "dest_closest_indicators" -t "dest_array_indicators" -t "od_aos_jsonb" -t "open_space_areas" -t "ind_summary" -t "exclusion_summary"  {db} > {out_file}'.format(db = db,db_user = db_user,out_file=out_file)    
    sp.call(command, shell=True,cwd=out_dir)   
    print("Done.")
    
    # # Note - i generated the create table commands with the following dump applied to Albury Wodonga:
    # out_file = 'ntnl_li_inds_schema.sql'.format(db)
    # print("\tCreating sql dump to: {}".format(os.path.join(out_dir,out_file))),
    # command = 'pg_dump -U {db_user} -h localhost --schema-only -t "parcel_indicators" -t "dest_closest_indicators" -t "dest_array_indicators" -t "od_aos_jsonb" -t "open_space_areas" -t "ind_summary" -t "exclusion_summary" {db} > {out_file}'.format(db = db,db_user = db_user,out_file=out_file)    
    # sp.call(command, shell=True,cwd=out_dir)   
    # print("Done.")
    
    # output to completion log    
    date_time = time.strftime("%Y%m%d-%H%M%S")
    duration = (time.time() - start)/60
  
    log_table = '''
        -- If log table doesn't exist, its created
        CREATE TABLE IF NOT EXISTS script_log
        (
        script varchar,
        task varchar,
        datetime_completed varchar,
        duration_mins numeric
        );
        -- Insert completed script details
        INSERT INTO script_log VALUES ($${}$$,$${}$$,$${}$$,{});
        '''.format(script,task,date_time,duration)
    try:
        curs.execute(log_table)
        conn.commit()
        print('''\nProcessing completed at {}\n- Task: {}\n- Duration: {:04.2f} minutes'''.format(date_time,task,duration))
    except:
        print("Error withoutput to script running log.  Has the database for this study region been created?")
        raise
    conn.close()

