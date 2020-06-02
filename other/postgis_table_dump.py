# Script:  postgis_table_dump.py
# Purpose: Output table
# Author:  Carl Higgs 
# Date:    3 August 2018

# Import custom variables for National Liveability indicator process
import psycopg2 
from _project_setup import *

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

localise_names = '''
ALTER TABLE IF EXISTS li_map_sa1      RENAME TO li_map_sa1_{locale}_{year};
ALTER TABLE IF EXISTS li_map_ssc      RENAME TO li_map_ssc_{locale}_{year};      
ALTER TABLE IF EXISTS li_map_lga      RENAME TO li_map_lga_{locale}_{year};      
ALTER TABLE IF EXISTS ind_description RENAME TO ind_description_{locale}_{year}; 
ALTER TABLE IF EXISTS boundaries_sa1  RENAME TO boundaries_sa1_{locale}_{year};  
ALTER TABLE IF EXISTS boundaries_ssc  RENAME TO boundaries_ssc_{locale}_{year};  
ALTER TABLE IF EXISTS boundaries_lga  RENAME TO boundaries_lga_{locale}_{year};  
DROP TABLE IF EXISTS urban_sos_{locale}_{year};
CREATE TABLE IF NOT EXISTS urban_sos_{locale}_{year} AS SELECT urban, ST_Transform(geom,4326) AS geom FROM study_region_urban;'''.format(locale = locale.lower(), year = year)

curs.execute(localise_names)
conn.commit()
  
print("Can you please run the following from the command prompt in the following directory: {locale_dir}".format(locale_dir = locale_dir))
print('''
pg_dump -U postgres -h localhost -W  -t "li_map_sa1_{locale}_{year}" -t "li_map_ssc_{locale}_{year}" -t "li_map_lga_{locale}_{year}" -t "ind_description_{locale}_{year}" -t "boundaries_sa1_{locale}_{year}" -t "boundaries_ssc_{locale}_{year}" -t "boundaries_lga_{locale}_{year}" -t "urban_sos_{locale}_{year}" {db} > li_map_{db}.sql
'''.format(locale = locale.lower(), year = year,db = db))

print('''
Also, can you send the following line of text to Carl please?
psql observatory < {locale_dir}/li_map_{db}.sql postgres
'''.format(locale_dir = locale_dir,db = db))