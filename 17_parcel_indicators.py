# Script:  17_parcel_indicators.py
# Purpose: Create parcel indicators for national liveability project
# Author:  Carl Higgs 
# Date:    20180717

import os
import sys
import time
import psycopg2 
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))
# Read in indicator description matrix
ind_matrix = pandas.read_csv(os.path.join(sys.path[0],'ind_study_region_matrix.csv'))
# Restrict to indicators associated with study region
ind_matrix = ind_matrix[ind_matrix['locale'].str.contains(locale)]
# Restrict to indicators with a defined source
ind_matrix = ind_matrix[pandas.notnull(ind_matrix['Source'])]


ind_matrix['indicators'] = ind_matrix['ind'] + ind_matrix['tags'].fillna('')
ind_matrix['null_queries'] = "SUM((" + ind_matrix['ind'] + " IS NULL::int)) AS " + ind_matrix['ind']

ind_list = ind_matrix['indicators'].tolist()

ind_queries = '\n'.join(ind_matrix['Query'])
ind_sources = '\n'.join(ind_matrix['Source'].unique())
null_query_summary = ',\n'.join("SUM(" + ind_matrix['indicators'] + " IS NULL::int) AS " + ind_matrix['indicators'])
null_query_combined = '+\n'.join("(" + ind_matrix['indicators'] + " IS NULL::int)")

create_parcel_indicators = '''
DROP TABLE IF EXISTS parcel_indicators;
CREATE TABLE parcel_indicators AS
SELECT
p.{id}                   ,
p.mb_code_20             ,
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
{indicators}             
p.geom                   
FROM
parcel_dwellings p                                                                                 
LEFT JOIN abs_linkage abs ON p.mb_code_20 = abs.mb_code_2016
LEFT JOIN non_abs_linkage non_abs ON p.{id} = non_abs.{id}
{sources}
'''.format(id = points_id, indicators = ind_queries, sources = ind_sources)


null_query_summary_table = '''
DROP TABLE IF EXISTS parcel_inds_null_summary; 
CREATE TABLE parcel_inds_null_summary AS
SELECT {null_query_summary} 
FROM parcel_indicators;
'''.format(null_query_summary = null_query_summary)

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

          
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

print("Creating compiled set of parcel level indicators...")
print("SQL query:")
print(create_parcel_indicators)
curs.execute(create_parcel_indicators)
conn.commit()
print("Done.")

print("Creating summary of nulls per indicator... ")
print("SQL query:")
print(null_query_summary_table)
curs.execute(null_query_summary_table)
conn.commit()
print("Done.")

print("Creating row-wise tally of nulls for each parcel...")
print("SQL query:")
print(null_query_combined_table)
curs.execute(null_query_combined_table)
conn.commit()
print("Done.\n")

df = pandas.read_sql_query('SELECT * FROM "parcel_inds_null_summary";',con=engine)
df = df.transpose()
df.columns = ['Null count']
print("Summary of nulls by {} variables for {} of {} in state of {}:".format(len(ind_list),region,locale,state))
print(df)

df2 = pandas.read_sql_query('SELECT * FROM "parcel_inds_null_tally";',con=engine)
print("Summary of row-wise null values across {} variables:".format(len(ind_list)))
print(df2['null_tally'].describe().round(2))

df.to_sql(name='parcel_ind_null_summary_t',con=engine,if_exists='replace')
df2['null_tally'].describe().round(2).to_sql(name='parcel_inds_null_tally_summary',con=engine,if_exists='replace')
ind_matrix.to_sql(name='ind_description',con=engine,if_exists='replace')

print("\n Nulls by indicator and Section of state")
for ind in ind_list:
  print("\n{}".format(ind))
  null_ind = pandas.read_sql_query("SELECT sos_name_2016, COUNT(*) null_count FROM parcel_indicators p LEFT JOIN parcel_sos sos ON p.gnaf_pid = sos.gnaf_pid WHERE {ind} IS NULL GROUP BY sos_name_2016;".format(ind = ind),con=engine)
  if len(null_ind) != 0:
    print(null_ind)
  if len(null_ind) == 0:
    print("No null values")


print("\nPostgresql summary tables containing the above were created:")
print("To view a description of all indicators for your region: SELECT * FROM ind_description;")
print("To view a summary of by variable name: SELECT * FROM parcel_ind_null_summary_t;")
print("To view a summary of row-wise null values: SELECT * FROM parcel_inds_null_tally_summary;")
print("To view a summary of null values for a particular indicator stratified by section of state:")
print(" SELECT sos_name_2016, COUNT(*) indicator_null_count FROM parcel_indicators p LEFT JOIN parcel_sos sos ON p.{id} = sos.{id} WHERE indicator IS NULL GROUP BY sos_name_2016;")

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
