# Script:  19_area_indicators.py
# Purpose: Create area level indicator tables
# Author:  Carl Higgs 
# Date:    20 July 2018

import os
import sys
import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create area level indicator tables for {}'.format(locale)

# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

# Indicator configuration sheet is 'df_inds', read in from config file in the config script

# Restrict to indicators associated with study region (except distance to closest dest indicators)
# the following two tables (indicators/measures, and distances to closest measures) will later be
# appended once the first table is expanded into soft and hard threshold indicator forms
ind_matrix = df_inds[df_inds['locale'].str.contains('|'.join([locale,'\*']))].copy()
ind_destinations = df_destinations[(df_destinations.locale == "*") | (df_destinations.locale == locale)].copy()
ind_destinations['destination'] = ind_destinations['destination'].apply(lambda x: "dist_m_{}".format(x))
ind_destinations = ind_destinations.set_index('destination')
ind_destinations.index.name = 'indicators'
ind_destinations = ind_destinations.loc[:,'unit_level_description':]

ind_matrix['order'] = list(ind_matrix.index)
ind_soft = ind_matrix.loc[ind_matrix.tags=='_{threshold}',:].copy()
ind_hard = ind_matrix.loc[ind_matrix.tags=='_{threshold}',:].copy()
ind_soft.replace(to_replace='{threshold}', value='soft', inplace=True,regex=True)
ind_hard.replace(to_replace='{threshold}', value='hard', inplace=True,regex=True)

ind_matrix = pandas.concat([ind_matrix,ind_soft,ind_hard], ignore_index=True).sort_values('ind')
ind_matrix.drop(ind_matrix[ind_matrix.tags == '_{threshold}'].index, inplace=True)
# Restrict to indicators with a defined query, or is the 'ULI' or 'SI Mix' indicator
ind_matrix = ind_matrix[pandas.notnull(ind_matrix['Query']) | ind_matrix.unit_level_description.isin(['Urban Liveability Index','Social infrastructure mix score (/15)'])]
ind_matrix.drop(ind_matrix[ind_matrix['updated?'] == 'n'].index, inplace=True)

# Make concatenated indicator and tag name (e.g. 'walk_14' + 'hard')
# Tags could be useful later as can allow to search by name for e.g. threshold type,
# or other keywords (policy, binary, obsolete, planned --- i don't know, whatever)
# These tags are tacked on the end of the ind name seperated with underscores
ind_matrix['indicators'] = ind_matrix['ind'] + ind_matrix['tags'].fillna('')

# Compile list of indicators
ind_matrix.sort_values('order', inplace=True)

# Create an indicators summary table
ind_matrix = ind_matrix.set_index('indicators')
ind_matrix = ind_matrix.append(ind_destinations)
ind_list = ind_matrix.index.values

indicator_tuples =  list(zip(ind_matrix.index,ind_matrix.agg_scale,ind_matrix.aggregate_description))
print("Creating Mesh Block level indicator table 'area_indicators_mb_json' (JSON nested summary statistics for each indicator at Mesh Block level)... "),
sql = '''
DROP TABLE IF EXISTS area_indicators_mb_json;
CREATE TABLE area_indicators_mb_json AS
SELECT a.mb_code_2016          ,
       a.mb_category_name_2016 ,
       t.study_region,
       t.locale,
       a.dwelling              ,
       a.person                ,
       a.sa1_maincode_2016     ,
       a.sa2_name_2016         ,
       a.sa3_name_2016         ,
       a.sa4_name_2016         ,
       a.gccsa_name_2016       ,
       a.state_name_2016       ,
       a.ssc_name_2016         ,
       a.lga_name_2016         ,
       a.ucl_name_2016         ,
       a.sos_name_2016         ,
       a.urban                 ,
       a.irsd_score            ,
       a.area_ha               ,
       jsonb_agg(
          to_jsonb(
              (SELECT i FROM
                  (SELECT
                      {indicators}
                  ) i))) AS indicators                ,
       sample_count                                   ,
       sample_count / a.area_ha AS sample_count_per_ha,
       a.geom                 
FROM area_linkage a 
LEFT JOIN (
    SELECT p.mb_code_2016,
           string_agg(DISTINCT(p.study_region),',')::varchar study_region,
           string_agg(DISTINCT(p.locale),',')::varchar locale,
           COUNT(p.*) AS sample_count       ,
          {jsonb_inds}
    FROM parcel_indicators p
    LEFT JOIN dest_closest_indicators USING(gnaf_pid)
    WHERE p.exclude IS NULL
    GROUP BY p.mb_code_2016) t USING (mb_code_2016)
WHERE a.irsd_score IS NOT NULL
  AND a.dwelling > 0
  AND a.urban = 'urban'
  AND a.study_region IS TRUE
  AND sample_count > 0
GROUP BY a.mb_code_2016,
         a.mb_category_name_2016 ,
         t.study_region,
         t.locale,
         a.dwelling              ,
         a.person                ,
         a.sa1_maincode_2016     ,
         a.sa2_name_2016         ,
         a.sa3_name_2016         ,
         a.sa4_name_2016         ,
         a.gccsa_name_2016       ,
         a.state_name_2016       ,
         a.ssc_name_2016         ,
         a.lga_name_2016         ,
         a.ucl_name_2016         ,
         a.sos_name_2016         ,
         a.urban                 ,
         a.irsd_score            ,
         a.area_ha               ,
         sample_count            ,
         sample_count_per_ha     ,
         a.geom
;
CREATE UNIQUE INDEX IF NOT EXISTS ix_area_indicators_mb_json ON  area_indicators_mb_json (mb_code_2016);
CREATE INDEX IF NOT EXISTS gix_area_indicators_mb_json ON area_indicators_mb_json USING GIST (geom);
'''.format(indicators = '"{}"'.format('","'.join(ind_list)),
           jsonb_inds = jsonb_summary_sql(indicator_tuples))
# print(sql)
curs.execute(sql)
conn.commit()
print("Done.")
# # The above can be used like so:
# ## SELECT mb_code_2016,sample_count,sample_count_per_ha,jsonb_pretty(indicators) AS indicators FROM abs_indicators LIMIT 1;
# ## 
# ## SELECT mb_code_2016, 
# ##        inds
# ## FROM abs_indicators, 
# ##     jsonb_array_elements(indicators) inds
# ## LIMIT 10;
# ## 
# ## SELECT mb_code_2016,
# ##        (ind->'walk_16')::jsonb->'mean' AS walk_16
# ## FROM abs_indicators,jsonb_array_elements(indicators) ind LIMIT 1;
# # # SELECT mb_code_2016, (ind->'walk_16')::jsonb->'mean' AS walk_16 FROM abs_indicators,jsonb_array_elements(indicators) ind LIMIT 1;
# ## sql = '''
# ## SELECT mb_code_2016,
# ##        {extract}
# ## FROM abs_indicators,jsonb_array_elements(indicators) ind LIMIT 1;
# ## '''.format(extract = ','.join(["(ind->'{i}')->'mean' AS {i}".format(i = i) for i in ind_list]))


print("Creating weighted area aggregate tables:")
for area in analysis_regions + ['study region']:   
    if area != 'study region':
        area_id = df_regions.loc[area,'id']
        abbrev = df_regions.loc[area,'abbreviation']
        include_region = 'study_region,'
    else: 
        area_id = 'study_region'
        abbrev  = 'region'
        include_region = ''
    if area != 'Section of State':
        pkey = area_id
    else: 
        pkey = '{},study_region'.format(area_id)
    for standard in ['dwelling','person']:
        print("  - li_inds_{}_{}".format(abbrev,standard))
        sql = '''
        DROP TABLE IF EXISTS {abbrev}_ind_{standard};
        DROP TABLE IF EXISTS li_inds_{abbrev}_{standard};
        CREATE TABLE li_inds_{abbrev}_{standard} AS
        SELECT 
        {area_code},
        {include_region}
        locale,
        SUM(dwelling) AS dwelling,
        SUM(person) AS person,
        SUM(sample_count) AS sample_count,
        SUM(sample_count)/SUM(area_ha) AS sample_count_per_ha,
        SUM(area_ha) AS area_ha,
        {extract},
        ST_Union(geom) AS geom
        FROM area_indicators_mb_json,
             jsonb_array_elements(indicators) ind
        GROUP BY {area_code},study_region,locale;
        '''.format(area_code = area_id,
                   abbrev = abbrev,
                   include_region = include_region,
                   extract = ','.join(['''
                       (CASE             
                            -- if there are no units (dwellings or persons) the indicator is null
                            WHEN COALESCE(SUM({standard}),0) = 0
                                THEN NULL
                            -- else, calculate the value of the unit weighted indicator
                            ELSE                             
                               (SUM({standard}*((ind->'{i}')->>'mean')::numeric)/SUM({standard}))::numeric
                          END) AS "{i}"
                   '''.format(i = i,standard = standard) for i in ind_list]),
                   standard = standard
                   )
        curs.execute(sql)
        conn.commit()
        sql = '''
        ALTER TABLE  li_inds_{abbrev}_{standard} ADD PRIMARY KEY ({pkey});
        '''.format(pkey = pkey,
                   abbrev = abbrev,
                   standard = standard)
        curs.execute(sql)
        conn.commit()

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
