# Purpose: Create area level indicator tables
# Author:  Carl Higgs 

import os
import sys
import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create area level indicator tables for {}'.format(locale)

# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db), 
                       use_native_hstore=False)

# Indicator configuration sheet is 'df_inds', read in from config file in the config script

# Restrict to indicators associated with study region (except distance to closest dest indicators)
# the following two tables (indicators/measures, and distances to closest measures) will later be
# appended once the first table is expanded into soft and hard threshold indicator forms
sql = '''
SELECT DISTINCT(table_name) 
  FROM information_schema.columns 
 WHERE table_schema = 'd_3200m_cl' 
 ORDER BY table_name;
'''
curs.execute(sql)
dest_tables = [x[0] for x in curs.fetchall()]

ind_destinations = df_destinations[(df_destinations.locale == "*") | (df_destinations.locale == locale)].copy()
ind_destinations = ind_destinations[ind_destinations['destination'].isin(dest_tables)]
ind_destinations['destination'] = ind_destinations['destination'].apply(lambda x: "dist_m_{}".format(x.lower()))
ind_destinations = ind_destinations.set_index('destination')
ind_destinations.index.name = 'indicators'
ind_destinations = ind_destinations.loc[:,'unit_level_description':]

# Indicator configuration sheet is 'df_inds', read in from config file in the config script
# Restrict to indicators associated with study region (except distance to closest dest indicators)
ind_matrix = df_inds[df_inds['locale'].str.contains('|'.join([locale,'\*']))].copy()

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
ind_matrix = ind_matrix[pandas.notnull(ind_matrix['updated?'])]

# Make concatenated indicator and tag name (e.g. 'walk_14' + 'hard')
# Tags could be useful later as can allow to search by name for e.g. threshold type,
# or other keywords (policy, binary, obsolete, planned --- i don't know, whatever)
# These tags are tacked on the end of the ind name seperated with underscores
ind_matrix['indicators'] = ind_matrix['ind'] + ind_matrix['tags'].fillna('')
# ind_matrix['sort_cat'] = pandas.Categorical(ind_matrix['ind'], categories=mylist, ordered=True)
# ind_matrix.sort_values('sort_cat', inplace=True)
# Compile list of indicators
ind_matrix.sort_values('order', inplace=True)

# Create an indicators summary table
ind_matrix = ind_matrix.set_index('indicators')
ind_matrix = ind_matrix.append(ind_destinations)
ind_list = ind_matrix.index.values

indicator_tuples =  list(zip(ind_matrix.index,ind_matrix.agg_scale,ind_matrix.aggregate_description))


print("Creating area indicator tables... ")
print("  - block...")
sql = '''
DROP TABLE IF EXISTS area_indicators_block;
CREATE TABLE area_indicators_block AS
SELECT f.blockid               ,
       f.blockname             ,
       f.wave                  ,
       p.study_region          ,
       p.locale                ,
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
       ST_Area(ST_Union(f.geom))*0.0001 area_ha,
       {indicators}             ,
       ST_Union(f.geom) geom
FROM boundaries.footprints f 
LEFT JOIN ind_point.parcel_indicators p ON f.blockid = p.blockid 
                                       AND f.wave = f.wave
                                       AND f.blockname = f.blockname
LEFT JOIN ind_point.dest_closest_indicators d ON p.{points_id} = d.{points_id}
WHERE p.exclude IS NULL
GROUP BY  f.blockid          ,  
          f.blockname        ,  
          f.wave             ,  
          p.study_region     ,  
          p.locale           ,  
          p.sa1_maincode_2016,  
          p.sa2_name_2016    ,  
          p.sa3_name_2016    ,  
          p.sa4_name_2016    ,  
          p.gccsa_name_2016  ,  
          p.state_name_2016  ,  
          p.ssc_name_2016    ,  
          p.lga_name_2016    ,  
          p.ucl_name_2016    ,  
          p.sos_name_2016    ,  
          p.urban            ,
          p.irsd_score               
ORDER BY f.blockid, f.wave    
;
CREATE INDEX IF NOT EXISTS area_indicators_block_idx ON  area_indicators_block (blockid);
CREATE INDEX IF NOT EXISTS area_indicators_block_gix ON area_indicators_block USING GIST (geom);
'''.format(points_id = points_id,
           indicators = indicator_summary_sql(indicator_tuples))
# print(sql)
curs.execute(sql)
conn.commit()

print("  - building...")
sql = '''
DROP TABLE IF EXISTS area_indicators_building;
CREATE TABLE area_indicators_building AS
SELECT f.buildlingno          ,
       f.buildname             ,
       f.wave                  ,
       p.study_region          ,
       p.locale                ,
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
       ST_Area(ST_Union(f.geom))*0.0001 area_ha,
       {indicators}             ,
       ST_Union(f.geom) geom
FROM boundaries.footprints f 
LEFT JOIN ind_point.parcel_indicators p ON f.buildlingno = p.buildlingno 
                                       AND f.wave = p.wave
                                       AND f.buildname = f.buildname
LEFT JOIN ind_point.dest_closest_indicators d ON p.{points_id} = d.{points_id}
WHERE p.exclude IS NULL
GROUP BY  f.buildlingno      ,
          f.buildname        ,
          f.wave             ,  
          p.study_region     ,  
          p.locale           ,  
          p.sa1_maincode_2016,  
          p.sa2_name_2016    ,  
          p.sa3_name_2016    ,  
          p.sa4_name_2016    ,  
          p.gccsa_name_2016  ,  
          p.state_name_2016  ,  
          p.ssc_name_2016    ,  
          p.lga_name_2016    ,  
          p.ucl_name_2016    ,  
          p.sos_name_2016    ,  
          p.urban            ,
       p.irsd_score               
ORDER BY f.buildlingno, f.wave    
;
CREATE INDEX IF NOT EXISTS area_indicators_block_idx ON  area_indicators_block (blockid);
CREATE INDEX IF NOT EXISTS area_indicators_block_gix ON area_indicators_block USING GIST (geom);
'''.format(points_id = points_id,
           indicators = indicator_summary_sql(indicator_tuples))
# print(sql)
curs.execute(sql)
conn.commit()

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
