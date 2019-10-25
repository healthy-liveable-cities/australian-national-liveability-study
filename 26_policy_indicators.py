# Purpose: Calculate policy indicators
# Author:  Carl Higgs 
# Date:    1 September 2019

import os
import sys
import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)

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


ind_policy = ind_matrix[ind_matrix.policy_locale.apply(lambda x: '{}'.format(x) not in ['NULL','nan'])].copy()
ind_policy = ind_policy.loc[:,['policy_reference','policy_wording','threshold_aggregate_description','agg_alt_variable','agg_standard','agg_split_greq','units']]

queries = []
for t in zip(ind_policy.index,ind_policy.agg_split_greq):
    ind_standard = ind_policy.loc[t[0],'agg_standard']
    if str(ind_policy.loc[t[0],'agg_alt_variable'])=='nan':
        sql = '''\n{standard}."{ind}" >= {split} AS {ind}'''.format(standard=ind_standard,ind = t[0],split=t[1])
    else:
        sql = '''\n{standard}."{ind}" >= {split} AS {source}'''.format(standard=ind_standard,ind = ind_policy.loc[t[0],'agg_alt_variable'],split=t[1],source=t[0])
    queries.append(sql)

print("Creating policy indicator tables:")
for area in analysis_regions + ['study region']:   
    if area != 'study region':
        area_id = df_regions.loc[area,'id']
        abbrev = df_regions.loc[area,'abbreviation']
        include_region = 'dwellings.study_region,'
    else: 
        area_id = 'study_region'
        abbrev  = 'region'
        include_region = ''
    if area != 'Section of State':
        pkey = area_id
        print("  - li_inds_{}_policy".format(abbrev))
        sql = '''
        DROP TABLE IF EXISTS li_inds_{abbrev}_policy;
        DROP TABLE IF EXISTS li_inds_{abbrev}_policy;
        CREATE TABLE li_inds_{abbrev}_policy AS
        SELECT 
        dwellings.{area_id},
        {include_region}
        dwellings.locale,
        dwellings.dwelling,
        dwellings.person,
        dwellings.sample_count,
        dwellings.sample_count_per_ha,
        dwellings.area_ha,
        {policy_indicators},
        dwellings.geom
        FROM li_inds_{abbrev}_dwelling dwellings
        LEFT JOIN li_inds_{abbrev}_person persons USING ({area_id});
        '''.format(area_id = area_id,
                abbrev = abbrev,
                include_region = include_region,
                policy_indicators = ','.join(queries)
                )
        curs.execute(sql)
        conn.commit()
        sql = '''
        ALTER TABLE  li_inds_{abbrev}_policy ADD PRIMARY KEY ({pkey});
        '''.format(pkey = pkey,
                abbrev = abbrev)
        curs.execute(sql)
        conn.commit()

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
