# Script:  NCPF_BITRE_summary.py
# Purpose: Create parcel indicators for national liveability project
# Author:  Carl Higgs 
# Date:    20180717

import time
import psycopg2 
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

from datetime import date
today = str(date.today())

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

# connect to postgresql database    
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

create_ncpf_mb_indicators = '''
DROP TABLE IF EXISTS ncpf_mb2;
CREATE TABLE ncpf_mb2 AS
SELECT a.mb_code_2016, 
	   a.dwelling,
	   a.person,
       -- note: the 'ind_hard' variable has already been evaluated against 400m for POS indicators
       -- Also, nulls signify no access, hence these are coalesced to be included as zero
	   a.dwelling * AVG(COALESCE((pt_any.distance < 400)::int,0))  AS dw_w_pt_any,
	   a.dwelling * AVG(COALESCE((pt_freq.distance < 400)::int,0)) AS dw_w_pt_freq,
	   a.dwelling * AVG(pos_400m_ncpf.any) AS dw_w_pos_any,
	   a.dwelling * AVG(pos_400m_ncpf.large) AS dw_w_pos_large
FROM parcel_dwellings p
LEFT JOIN abs_linkage a ON p.mb_code_20 = a.mb_code_2016
LEFT JOIN (SELECT * FROM od_closest     WHERE dest_class = 'gtfs_2018_stops') pt_any   ON p.gnaf_pid = pt_any.gnaf_pid 
LEFT JOIN (SELECT * FROM od_closest     WHERE dest_class = 'gtfs_2018_stop_30_mins_final') pt_freq  ON p.gnaf_pid = pt_freq.gnaf_pid 
LEFT JOIN pos_400m_ncpf ON p.gnaf_pid = pos_400m_ncpf.gnaf_pid 
WHERE NOT EXISTS (SELECT 1 
                    FROM excluded_parcels x 
                   WHERE x.gnaf_pid = p.gnaf_pid)
GROUP BY mb_code_2016, dwelling, person;
''' 
 
 
print("Create NCPF mesh block results... "),
curs.execute(create_ncpf_mb_indicators)
conn.commit()
print("Done.")

create_ncpf_region_summary = '''
DROP TABLE IF EXISTS ncpf_region2;
CREATE TABLE ncpf_region2 AS
SELECT SUM(dwelling)::int AS dwellings,
	   SUM(person)::int   AS persons,
	   ROUND((100*SUM(dw_w_pt_any   )/SUM(dwelling))::numeric, 2) AS pct_pt_any   ,
	   ROUND((100*SUM(dw_w_pt_freq  )/SUM(dwelling))::numeric, 2) AS pct_pt_freq  ,
	   ROUND((100*SUM(dw_w_pos_any  )/SUM(dwelling))::numeric, 2) AS pct_pos_any  ,
	   ROUND((100*SUM(dw_w_pos_large)/SUM(dwelling))::numeric, 2) AS pct_pos_large
FROM ncpf_mb2;
'''
print("Create NCPF region results... "),
curs.execute(create_ncpf_region_summary)
conn.commit()
print("Done.")

summary = pandas.read_sql_query('''SELECT * FROM ncpf_region2''',con=engine)
print(summary)
values = [locale] + summary.values.tolist()[0]
output = os.path.join(folderPath,'ncpf_region2_summary_{}_{}.csv'.format(responsible[locale],today))
header = '''{},{},{},{},{},{},{}\n'''.format('locale','Dwellings','Persons','% PT (any)','% PT (frequent)','% POS (any)','% POS (large)')

if not os.path.exists(output):
   with open(output, "w") as f:
     f.write(header)
with open(output, "a") as f:
  f.write('''{},{},{},{},{},{},{}\n'''.format(*values))
# # output to completion log    
# script_running_log(script, task, start, locale)
# conn.close()
