# Script:  17_parcel_indicators.py
# Purpose: Create parcel indicators for national liveability project
# Author:  Carl Higgs 
# Date:    20190530

import time
import psycopg2 
import numpy as np
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

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
ind_matrix = df_inds[df_inds['locale'].str.contains('|'.join([locale,'\*']))].copy()

# Get a list of destinations processed within this region for distance to closest
# sql = '''SELECT DISTINCT(dest_name) dest_name FROM od_closest ORDER BY dest_name;'''
sql = '''SELECT dest_name FROM dest_type ORDER BY dest_name;'''
curs.execute(sql)
categories = [x[0] for x in curs.fetchall()]

# # get the set of distance to closest regions which match for this region
# destinations = df_inds[df_inds['ind'].str.contains('destinations')]
# current_categories = [x for x in categories if 'distance_m_{}'.format(x) in destinations.ind_plain.str.encode('utf8').tolist()]
# ind_matrix = ind_matrix.append(destinations[destinations['ind_plain'].str.replace('distance_m_','').str.contains('|'.join(current_categories))])
ind_matrix['order'] = ind_matrix.index
ind_soft = ind_matrix.loc[ind_matrix.tags=='_{threshold}',:].copy()
ind_hard = ind_matrix.loc[ind_matrix.tags=='_{threshold}',:].copy()
ind_soft.replace(to_replace='{threshold}', value='soft', inplace=True,regex=True)
ind_hard.replace(to_replace='{threshold}', value='hard', inplace=True,regex=True)

ind_matrix = pandas.concat([ind_matrix,ind_soft,ind_hard], ignore_index=True).sort_values('ind')
ind_matrix.drop(ind_matrix[ind_matrix.tags == '_{threshold}'].index, inplace=True)
# Restrict to indicators with a defined query
ind_matrix = ind_matrix[pandas.notnull(ind_matrix['Query'])]
ind_matrix.drop(ind_matrix[ind_matrix['updated?'] == 'n'].index, inplace=True)

# Make concatenated indicator and tag name (e.g. 'walk_14' + 'hard')
# Tags could be useful later as can allow to search by name for e.g. threshold type,
# or other keywords (policy, binary, obsolete, planned --- i don't know, whatever)
# These tags are tacked on the end of the ind name seperated with underscores
ind_matrix['indicators'] = ind_matrix['ind'] + ind_matrix['tags'].fillna('')
# ind_matrix['sort_cat'] = pandas.Categorical(ind_matrix['ind'], categories=mylist, ordered=True)
# ind_matrix.sort_values('sort_cat', inplace=True)
# Compile list of indicators
ind_matrix.sort_values('order', inplace=True)
ind_list = ind_matrix['indicators'].tolist()

# Compile string of queries, and of unique sources to plug in SQL table creation query
ind_queries = '\n'.join(ind_matrix['Query'] +' AS ' + ind_matrix['indicators']+',')
ind_sources = '\n'.join(ind_matrix['Source'].unique())

print("Creating compiled set of parcel level indicators..."),   
# Define parcel level indicator table creation query
# Note that we modify inds slightly later when aggregated to reflect cutoffs etc
create_parcel_indicators = '''
DROP TABLE IF EXISTS parcel_indicators;
CREATE TABLE parcel_indicators AS
SELECT
p.{id}                    ,
p.count_objectid          ,
p.point_x                 ,
p.point_y                 ,
p.hex_id                  ,
'{full_locale}'::text AS study_region,
'{locale}'::text AS locale      ,
area.mb_code_2016         ,
area.mb_category_name_2016,
area.sa1_maincode_2016    ,
area.sa2_name_2016        ,
area.sa3_name_2016        ,
area.sa4_name_2016        ,
area.gccsa_name_2016      ,
area.state_name_2016      ,
area.ssc_name_2016        ,
area.lga_name_2016        ,
area.ucl_name_2016        ,
area.sos_name_2016        ,
area.urban                ,
area.irsd_score           ,
e.exclude                 ,
{indicators}            
p.geom                   
FROM
parcel_dwellings p                                                                                 
LEFT JOIN area_linkage area ON p.mb_code_20 = area.mb_code_2016
LEFT JOIN (SELECT {id}, string_agg(indicator,',') AS exclude FROM excluded_parcels GROUP BY {id}) e 
    ON p.{id} = e.{id}
{sources};
CREATE UNIQUE INDEX IF NOT EXISTS ix_parcel_indicators ON  parcel_indicators ({id});
CREATE INDEX IF NOT EXISTS gix_parcel_indicators ON parcel_indicators USING GIST (geom);
'''.format(id = points_id, 
           indicators = ind_queries, 
           sources = ind_sources, 
           full_locale = full_locale,
           locale = locale)

# print("SQL query:")
# print(create_parcel_indicators)
curs.execute(create_parcel_indicators)
conn.commit()
print(" Done.")

table = 'dest_distance_m'
sql = '''
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = '{table}' 
AND column_name != '{id}';
'''.format(id = points_id.lower(), table = table)
curs.execute(sql)
destinations = ','.join(['d."{dest}" AS "dist_m_{dest}"'.format(dest = x[0]) for x in curs.fetchall()])

print("Creating distance to closest measures with classification data..."),
dest_closest_indicators = '''
DROP TABLE IF EXISTS dest_closest_indicators;
CREATE TABLE dest_closest_indicators AS
SELECT
{id}                    ,
p.count_objectid        ,
p.point_x               ,
p.point_y               ,
p.hex_id                ,
'{full_locale}'::text AS study_region,
'{locale}'::text AS locale      ,
p.mb_code_2016          ,
p.mb_category_name_2016 ,
p.sa1_maincode_2016     ,
p.sa2_name_2016         ,
p.sa3_name_2016         ,
p.sa4_name_2016         ,
p.gccsa_name_2016       ,
p.state_name_2016       ,
p.ssc_name_2016         ,
p.lga_name_2016         ,
p.ucl_name_2016         ,
p.sos_name_2016         ,
p.urban                 ,
p.irsd_score            ,
p.exclude               ,
{d}                     ,
p.geom                   
FROM
parcel_indicators p                                                                                 
LEFT JOIN dest_distance_m d
USING ({id});
CREATE UNIQUE INDEX IF NOT EXISTS ix_dest_closest_indicators ON  dest_closest_indicators ({id});
CREATE INDEX IF NOT EXISTS gix_dest_closest_indicators ON dest_closest_indicators USING GIST (geom);
'''.format(id = points_id, 
           d = destinations, 
           full_locale = full_locale,
           locale = locale)
curs.execute(dest_closest_indicators)
conn.commit()
print(" Done.")

table = 'dest_distances_cl_3200m'
sql = '''
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = '{table}' 
AND column_name != '{id}';
'''.format(id = points_id.lower(), table = table)
curs.execute(sql)
destinations = ','.join(['d."{dest}" AS "dist_m_{dest}"'.format(dest = x[0]) for x in curs.fetchall()])

print("Creating distance array measures with classification data..."),
dest_array_indicators = '''
DROP TABLE IF EXISTS dest_array_indicators;
CREATE TABLE dest_array_indicators AS
SELECT
{id}                    ,
p.count_objectid        ,
p.point_x               ,
p.point_y               ,
p.hex_id                ,
'{full_locale}'::text AS study_region,
'{locale}'::text AS locale      ,
p.mb_code_2016          ,
p.mb_category_name_2016 ,
p.sa1_maincode_2016     ,
p.sa2_name_2016         ,
p.sa3_name_2016         ,
p.sa4_name_2016         ,
p.gccsa_name_2016       ,
p.state_name_2016       ,
p.ssc_name_2016         ,
p.lga_name_2016         ,
p.ucl_name_2016         ,
p.sos_name_2016         ,
p.urban                 ,
p.irsd_score            ,
p.exclude               ,
{d}                     ,
p.geom                   
FROM
parcel_indicators p                                                                                 
LEFT JOIN dest_distances_cl_3200m d
USING ({id});
CREATE UNIQUE INDEX IF NOT EXISTS dest_array_indicators_idx ON  dest_array_indicators ({id});
'''.format(id = points_id, 
           d = destinations, 
           full_locale = full_locale,
           locale = locale)

curs.execute(dest_array_indicators)
conn.commit()
print(" Done.")

sql = '''
DROP TABLE IF EXISTS exclusion_summary;
CREATE TABLE exclusion_summary AS
SELECT '{}'::text AS locale,
       COALESCE(exclude,'Included (not excluded)') AS "Exclusions",
       count(*) 
FROM parcel_indicators 
GROUP BY exclude 
ORDER BY count DESC;
'''.format(locale)
curs.execute(sql)
conn.commit()
df = pandas.read_sql_query('''SELECT "Exclusions",count FROM exclusion_summary''',
                           con=engine,
                           index_col='Exclusions')
pandas.set_option('display.max_colwidth', -1)
print("\n")
print(df)


# Drop index for ind_description table if it exists; 
# this causes an error when (re-)creating the ind_description table if index exists
curs.execute('DROP INDEX IF EXISTS ix_ind_description_index;')
conn.commit()
ind_matrix.to_sql(name='ind_description',con=engine,if_exists='replace')

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
