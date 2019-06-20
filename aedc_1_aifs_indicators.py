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
   python 17_aedc_indicators_aifs.py Carl
   python 17_aedc_indicators_aifs.py albury_wodonga
   python 17_aedc_indicators_aifs.py albury_wodonga ballarat cairns launceston newcastle_maitland perth adelaide
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
    
# Indicator configuration
ind_aedc = pandas.read_excel(xls, 'aedc')
ind_aedc = ind_aedc[ind_aedc.apply (lambda r: ('{}'.format(r.table)!='nan') and ('{}'.format(r['AEDC 1 - AIFS linkage'])!='nan'),axis = 1)].copy()

indicators = '\n'.join(ind_aedc.apply(lambda r: '{table}.{variable} AS {aedc_name},'.format(table = r.table, variable = r.variable, aedc_name = r.aedc_name),axis = 1).unique())

joins = '\n'.join(ind_aedc.apply(lambda r: 'LEFT JOIN {table} ON {join}."{join_id}" = {table}."{table_id}"'.format(table = r.table, table_id = r.table_id, join = r.join, join_id = r.join_id),axis = 1).unique())

out_dir = os.path.join(folderPath,'study_region','aedc')
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

    print("\tCreating compiled set of parcel level indicators..."),   
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

    print("\tCreating study region area summary table..."),  
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

    print("\tEnsuring locale is recorded in aos_acara_naplan table..."),  
    sql = '''
    ALTER TABLE aos_acara_naplan ADD COLUMN IF NOT EXISTS locale text;
    ALTER TABLE aos_acara_naplan ADD COLUMN IF NOT EXISTS geom geometry;
    UPDATE aos_acara_naplan SET locale = '{locale}';
    UPDATE aos_acara_naplan a
       SET geom = s.geom
      FROM acara_schools_naplan_linkage_2017 s
     WHERE a.acara_school_id = s.acara_school_id;
    '''.format(locale = locale)
    curs.execute(sql)
    conn.commit()
    print(" Done.")

    out_file = 'aedc_aifs_{}_Fc.sql'.format(db)
    print("\tCreating sql dump to: {}".format(os.path.join(out_dir,out_file))),
    command = 'pg_dump -U {db_user} -h localhost -Fc -t "study_region_locale" -t "aedc_indicators_aifs" -t "exclusion_summary"  -t "open_space_areas" -t "aos_acara_naplan" {db} > {out_file}'.format(db = db,db_user = db_user,out_file=out_file)    
    sp.call(command, shell=True,cwd=out_dir)   
    print("Done.")
    
    # Note - i genereated the create table commands with the following dump applied to Albury Wodonga:
    out_file = 'aedc_aifs_schema.sql'.format(db)
    print("\tCreating sql dump to: {}".format(os.path.join(out_dir,out_file))),
    command = 'pg_dump -U {db_user} -h localhost --schema-only -t "study_region_locale" -t "aedc_indicators_aifs" -t "exclusion_summary"  -t "open_space_areas" -t "aos_acara_naplan" {db} > {out_file}'.format(db = db,db_user = db_user,out_file=out_file)    
    sp.call(command, shell=True,cwd=out_dir)   
    print("Done.")
    
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

