# Script:  aedc_national_collation.py
# Purpose: Collate study region indicator output sql dumps
# Author:  Carl Higgs 
# Date:    20190703
# Note:    Assumes the li_australia_2018 database has been created.  
import time
import psycopg2 
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

# Connect to postgresql database     
db = 'li_australia_2018'
year = 2018
dh_dir = os.path.join(folderPath,'study_region','dh_inds')
print("This script assumes the database {db} has been created!\n".format(db = db))
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

out_file = 'dh_inds_sa1_{}.csv'.format(time.strftime('%Y%d%m'))
dh_inds_csv = []
for root, dirs, files in os.walk('D:/ntnl_li_2018_template/data/study_region/dh_inds'):
    if file.endswith('.csv') and file!='':
        dh_inds_csv.append(file)

print("\nMerging csv source data with area linkage table:")
dfs = [pandas.read_csv(f, 
                       compression='infer', 
                       header=0, 
                       sep=',', 
                       quotechar='"') 
                  for f in dh_inds_csv]
df = pandas.concat(dfs).sort_index()
df.to_csv(,index=False)
print("Done.")                   


# output to completion log    
script_running_log(script, task, start, locale)
conn.close()


