# Script:  17_parcel_indicators.py
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
ind_matrix = df_inds[df_inds['locale'].str.contains('|'.join([locale,'\*']))]

# Get a list of destinations processed within this region for distance to closest
# sql = '''SELECT DISTINCT(dest_name) dest_name FROM od_closest ORDER BY dest_name;'''
sql = '''SELECT dest_name FROM dest_type ORDER BY dest_name;'''
curs.execute(sql)
categories = [x[0] for x in curs.fetchall()]

# get the set of distance to closest regions which match for this region
destinations = df_inds[df_inds['ind'].str.contains('destinations')]
current_categories = [x for x in categories if 'distance_m_{}'.format(x) in destinations.ind_plain.str.encode('utf8').tolist()]
ind_matrix = ind_matrix.append(destinations[destinations['ind_plain'].str.replace('distance_m_','').str.contains('|'.join(current_categories))])
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

print("Creating compiled set of parcel level indicators...")   
# Define parcel level indicator table creation query
# Note that we modify inds slightly later when aggregated to reflect cutoffs etc
create_parcel_indicators = '''
DROP TABLE IF EXISTS parcel_indicators;
CREATE TABLE parcel_indicators AS
SELECT
p.{id}                   ,
p.count_objectid         ,
p.point_x                ,
p.point_y                ,
p.hex_id                 ,
abs.mb_code_2016         ,
abs.mb_category_name_2016,
abs.dwelling             ,
abs.person               ,
abs.sa1_maincode         ,
abs.sa2_name_2016        ,
abs.sa3_name_2016        ,
abs.sa4_name_2016        ,
abs.gccsa_name           ,
abs.state_name           ,
non_abs.ssc_code_2016    ,
non_abs.ssc_name_2016    ,
non_abs.lga_code_2016    ,
non_abs.lga_name_2016    ,
sos.sos_name_2016        ,
{indicators}             
p.geom                   
FROM
parcel_dwellings p                                                                                 
LEFT JOIN abs_linkage abs ON p.mb_code_20 = abs.mb_code_2016
LEFT JOIN non_abs_linkage non_abs ON p.{id} = non_abs.{id}
LEFT JOIN parcel_sos sos ON p.{id} = sos.{id}
{sources}
'''.format(id = points_id, indicators = ind_queries, sources = ind_sources)

# print("SQL query:")
# print(create_parcel_indicators)
curs.execute(create_parcel_indicators)
conn.commit()
print("Done.")

# Create an indicators summary table
print("Data checking\n")
ind_summary = ind_matrix.set_index('indicators')
ind_summary_urban = ind_matrix.set_index('indicators')['unit_level_description'].to_frame()
ind_summary_not_urban = ind_summary_urban

print("Create tables for data checking purposes...")
# Generate strings for checking nulls: by column (indicator), and by row
# null_query_summary = ',\n'.join("SUM(" + ind_matrix['indicators'] + " IS NULL::int) AS " + ind_matrix['indicators'])
query_summaries = {
   'mean' :',\n'.join("ROUND(AVG("    + ind_matrix['indicators'] + ")::numeric,2) AS " + ind_matrix['indicators']),
   'sd'   :',\n'.join("ROUND(STDDEV(" + ind_matrix['indicators'] + ")::numeric,2) AS " + ind_matrix['indicators']),
   'min'  :',\n'.join("ROUND(MIN("    + ind_matrix['indicators'] + ")::numeric,2) AS " + ind_matrix['indicators']),
   'max'  :',\n'.join("ROUND(MAX("    + ind_matrix['indicators'] + ")::numeric,2) AS " + ind_matrix['indicators']),
   'p2.5' :',\n'.join("ROUND(percentile_cont(0.025) WITHIN GROUP (ORDER BY "+ ind_matrix['indicators'] + ")::numeric,2) AS " + ind_matrix['indicators']),
   'p25'  :',\n'.join("ROUND(percentile_cont(0.25 ) WITHIN GROUP (ORDER BY "+ ind_matrix['indicators'] + ")::numeric,2) AS " + ind_matrix['indicators']),
   'p50'  :',\n'.join("ROUND(percentile_cont(0.5  ) WITHIN GROUP (ORDER BY "+ ind_matrix['indicators'] + ")::numeric,2) AS " + ind_matrix['indicators']),
   'p75'  :',\n'.join("ROUND(percentile_cont(0.75 ) WITHIN GROUP (ORDER BY "+ ind_matrix['indicators'] + ")::numeric,2) AS " + ind_matrix['indicators']),
   'p97.5':',\n'.join("ROUND(percentile_cont(0.975) WITHIN GROUP (ORDER BY "+ ind_matrix['indicators'] + ")::numeric,2) AS " + ind_matrix['indicators']),
   'count':',\n'.join("COUNT(*) AS " + ind_matrix['indicators']),
   'nulls':',\n'.join("SUM("  + ind_matrix['indicators'] + " IS NULL::int) AS " + ind_matrix['indicators']),
    }

for summary in query_summaries:
    # get null values
    df = pandas.read_sql_query('''SELECT {} FROM parcel_indicators;'''.format(query_summaries[summary]),con=engine)
    df = df.transpose()
    df.columns=[summary]
    ind_summary = ind_summary.join(df, how='left')
    # get urban null values
    df = pandas.read_sql_query('''SELECT {} FROM parcel_indicators WHERE sos_name_2016 IN ('Major Urban','Other Urban');'''.format(query_summaries[summary]),con=engine)
    df = df.transpose()
    df.columns=[summary]
    ind_summary_urban = ind_summary_urban.join(df, how='left')
    # get not urban null values
    df = pandas.read_sql_query('''SELECT {} FROM parcel_indicators WHERE sos_name_2016 NOT IN ('Major Urban','Other Urban');'''.format(query_summaries[summary]),con=engine)
    df = df.transpose()
    df.columns=[summary]
    ind_summary_not_urban = ind_summary_not_urban.join(df, how='left')

ind_summary['null_pct'] = ind_summary.apply (lambda row: 100*( row['nulls'] / np.float64(row['count'])) , axis=1)
ind_summary_urban['null_pct'] = ind_summary_urban.apply (lambda row: 100*( row['nulls'] / np.float64(row['count'])) , axis=1)
ind_summary_not_urban['null_pct'] = ind_summary_not_urban.apply (lambda row: 100*( row['nulls'] / np.float64(row['count'])) , axis=1)

# Get overall count to add to urban and not urban for percentage contributions
overall_count = ind_summary['count'].to_frame()    
overall_count.columns = ['overall_count']
ind_summary_urban = ind_summary_urban.join(overall_count,how='left')
ind_summary_not_urban = ind_summary_not_urban.join(overall_count,how='left')
# calculate percentage
ind_summary['count_pct'] = ind_summary.apply (lambda row: 100 * row['count'] / np.float64(row['count'] ) , axis=1).round(2)
ind_summary_urban['count_pct'] = ind_summary_urban.apply (lambda row: 100 * row['count'] / np.float64(row['overall_count'] ) , axis=1).round(2)
ind_summary_not_urban['count_pct'] = ind_summary_not_urban.apply (lambda row: 100 * row['count'] / np.float64(row['overall_count'] ) , axis=1).round(2)
ind_summary.to_sql(name='ind_summary',con=engine,if_exists='replace')
print('     - ind_summary')
ind_summary_urban.to_sql(name='ind_summary_urban',con=engine,if_exists='replace')
print('     - ind_summary_urban')
ind_summary_not_urban.to_sql(name='ind_summary_not_urban',con=engine,if_exists='replace')
print('     - ind_summary_not_urban')
print("Done.")
print("\nPlease review the following indicator summary and consider any oddities:"),
# print for diagnostic purposes
variables = ['mean','sd','min','p25','p50','p75','max','nulls','null_pct','count','count_pct']
for i in ind_summary.index:
    print('\n{}:'.format(ind_summary.loc[i]['unit_level_description']))
    print('     {}'.format(i))
    summary = list(ind_summary.loc[i,variables].values)
    summary_urban = list(ind_summary_urban.loc[i,variables].values)
    summary_not_urban = list(ind_summary_not_urban.loc[i,variables].values)
    print('            {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}'.format(*variables))
    print('Overall     {:10} {:10} {:10} {:10} {:10} {:10} {:10} {:10} {:10.2} {:10} {:10.2}'.format(*summary))
    print('Urban       {:10} {:10} {:10} {:10} {:10} {:10} {:10} {:10} {:10.2} {:10} {:10.2}'.format(*summary_urban))
    print('Not urban   {:10} {:10} {:10} {:10} {:10} {:10} {:10} {:10} {:10.2} {:10} {:10.2}'.format(*summary_not_urban)
)
print("Creating row-wise tally of nulls for each parcel...")
null_query_combined = '+\n'.join("(" + ind_matrix['indicators'] + " IS NULL::int)")

null_query_combined_table = '''
DROP TABLE IF EXISTS parcel_inds_null_tally; 
CREATE TABLE parcel_inds_null_tally AS
SELECT {id},
       {null_query_combined} AS null_tally,
       {total_inds} AS total_inds
FROM parcel_indicators;
'''.format(id = points_id,
           null_query_combined = null_query_combined,
           total_inds = len(ind_list))


# print("SQL query:")
# print(null_query_combined_table)
curs.execute(null_query_combined_table)
conn.commit()
print("Done.\n")

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
