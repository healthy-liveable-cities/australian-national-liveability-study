# Script:  LiveWorkSameSA3_bySA1.py
# Purpose: Process a network matrix csv file w/ total column
#          NOTE: MAKE SURE THAT COLUMNS CORRESPOND TO ROWS, SUCH THAT DIAGONAL IS THE INTERSECTION OF LIVE AND WORK!!!!
#              otherwise, results will be false.
#          OUTPUT:
#          [id,  diagonal as a numerator column, total as denominator column, proportion = numerator/denominator]
#          Specifically, this is used to process ABS derived files:
#              ** UsualResidence_by_PlaceOfWork > LiveWorkSameSA3  **
# Author:  Carl Higgs
# Date:    7/12/2016


#import packages
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

# Specify file names - assumed to be csv files

indexName = 'sa1_7digitcode_2016'

df = pandas.read_csv('D:/ABS/data/2016/abs_liveability/SA1 (UR) by SA3 (POW) - cleaned_20190712.csv', index_col=0)
df = df.reset_index()
df = pandas.melt(df, id_vars=['sa1_7digitcode_2016'], value_vars=[x for x in df.columns if x!='sa1_7digitcode_2016'],var_name='sa3_work',value_name='count')
df = df.astype(np.int64)

# df2 = pandas.read_csv('D:/ABS/data/abs_liveability/sa1_live_sa3_work_long_20190712/sa1_live_sa3_work_long.csv', index_col=0,skiprows )

# Get SA1 to SA3 look up table
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

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
live_work= live_work.groupby('local').count.agg(['sum'])
live_work= live_work.groupby(['sa1_7digitcode_2016','local'])['count'].agg(['sum']).unstack(fill_value = np.nan)
live_work.columns = live_work.columns.droplevel()
live_work = live_work.reset_index()
live_work = live_work.set_index('sa1_7digitcode_2016')
live_work = live_work.fillna(0)
live_work = live_work.astype(np.int64)
live_work['total'] = live_work.apply(lambda x: x[False]+x[True],axis=1)
live_work['pct_live_work_local_area'] = live_work.apply(lambda x: 100*(x[True]/float(x['total'])),axis=1)

live_work.to_sql('live_sa1_work_sa3', con=engine, if_exists='replace')
# output to completion log
script_running_log(script, task, start)
conn.close()
