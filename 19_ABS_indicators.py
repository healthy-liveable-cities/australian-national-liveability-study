# Script:  ABS_indicators.py
# Purpose: Process ABS indicators (e.g. affordable housing; live work same area)
# Author:  Carl Higgs
# Date:    2020-01-13

#import packages
import os
import sys
import time
import numpy as np
import psycopg2 
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

area_codes  = pandas.read_sql('SELECT DISTINCT(sa1_7digitcode_2016)::int FROM area_linkage',engine)


# import affordable housing indicator
print("Affordable housing indicator... "),
affordable_housing = pandas.read_csv('../data/ABS/derived/abs_2016_sa1_housing_3040_20190712.csv', index_col=0)
affordable_housing = affordable_housing.loc[area_codes['sa1_7digitcode_2016']]
affordable_housing.to_sql('abs_ind_30_40', con=engine, if_exists='replace')
print("Done.")

# import mode of transport to work data
print("Mode of transport to work indicators... "),
mtwp = pandas.read_csv('../data/ABS/derived/abs_2016_sa1_mtwp_cleaned.csv', index_col=0)
mtwp = mtwp.loc[area_codes['sa1_7digitcode_2016']]
modes = ['active_transport','public_transport','vehicle','other_mode']
mtwp['total_employed_travelling'] = mtwp[modes].sum(axis=1)
for mode in modes[:-1]:
  # percentage of employed persons travelling to work using this mode
  mtwp['pct_{}'.format(mode)] = 100*mtwp[mode]/mtwp['total_employed_travelling']

mtwp.to_sql('abs_mode_of_transport_to_work', con=engine, if_exists='replace')
print("Done.")

# import proportion renting indicator
print("Proportion renting indicator... "),
pct_renting = pandas.read_csv('../data/ABS/derived/abs_au_2016_tenure_type_by_sa1.csv', index_col=0)
pct_renting = pct_renting.loc[area_codes['sa1_7digitcode_2016']]
pct_renting['valid_total'] = pct_renting['Total'] - pct_renting['Not stated'] - pct_renting['Not applicable']
pct_renting['pct_renting'] = 100*pct_renting['Rented']/pct_renting['valid_total']
pct_renting.to_sql('abs_pct_renting', con=engine, if_exists='replace')
print("Done.")


print("Live and work in same local area indicator... "),
indexName = 'sa1_7digitcode_2016'
df = pandas.read_csv('../data/ABS/derived/SA1 (UR) by SA3 (POW) - cleaned_20190712.csv', index_col=0)
df = df.reset_index()
df = pandas.melt(df, id_vars=['sa1_7digitcode_2016'], value_vars=[x for x in df.columns if x!='sa1_7digitcode_2016'],var_name='sa3_work',value_name='count')
df = df.astype(np.int64)

# Get SA1 to SA3 look up table
sql = '''
SELECT DISTINCT(sa1_7digitcode_2016),
       sa3_code_2016 AS sa3_live 
  FROM sa1_2016_aust 
GROUP BY sa1_7digitcode_2016, 
         sa3_live 
ORDER BY sa1_7digitcode_2016;
'''
curs.execute(sql)
area_lookup = pandas.DataFrame(curs.fetchall(), columns=['sa1_7digitcode_2016', 'sa3_live'])
area_lookup = area_lookup.astype(np.int64)

# Merge lookup with usual residence (UR) by place of work (POW)
live_work = pandas.merge(df, area_lookup, how='left', left_on='sa1_7digitcode_2016', right_on='sa1_7digitcode_2016')

# remove those areas where no one works
live_work = live_work[live_work['count']!=0]
live_work['local'] = live_work.apply(lambda x: x.sa3_live==x.sa3_work,axis=1)
live_work= live_work.groupby(['sa1_7digitcode_2016','local'])['count'].agg(['sum']).unstack(fill_value = np.nan)
live_work.columns = live_work.columns.droplevel()
live_work = live_work.reset_index()
live_work = live_work.set_index('sa1_7digitcode_2016')

# filter down to area codes in region of interest
local_live_work = live_work.loc[area_codes['sa1_7digitcode_2016']]

live_work = live_work.fillna(0)
live_work = live_work.astype(np.int64)
live_work['total'] = live_work.apply(lambda x: x[False]+x[True],axis=1)
live_work['pct_live_work_local_area'] = live_work.apply(lambda x: 100*(x[True]/float(x['total'])),axis=1)

live_work.to_sql('live_sa1_work_sa3', con=engine, if_exists='replace')
print("Done.")


print("Compile ABS indicators at SA1 level... "),
dfs = [affordable_housing[['pct_30_40_housing']],
       mtwp[['pct_active_transport','pct_public_transport','pct_vehicle']],
       pct_renting[['pct_renting']],
       live_work[['pct_live_work_local_area']]
       ]
abs_indicators = dfs[0].join(dfs[1:])

print("Done.")

# output to completion log
script_running_log(script, task, start)
conn.close()
