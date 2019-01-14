# Script:  NCPF_BITRE_summary.py
# Purpose: Create parcel indicators for national liveability project
# Author:  Carl Higgs 
# Date:    20180717

import time
import psycopg2 
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

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
DROP TABLE IF EXISTS ncpf_mb3;
CREATE TABLE ncpf_mb3 AS
SELECT a.mb_code_2016, 
	   a.dwelling,
	   a.person,
	   a.dwelling * AVG((pt_any.distance    < 400)::int)  AS dw_w_pt_any,
	   a.dwelling * AVG((pt_freq.distance   < 400)::int) AS dw_w_pt_freq,
	   a.dwelling * AVG((pos_any.distance   < 400)::int) AS dw_w_pos_any,
	   a.dwelling * AVG((pos_large.distance < 400)::int) AS dw_w_pos_large
FROM parcel_dwellings p
LEFT JOIN abs_linkage a ON p.mb_code_20 = a.mb_code_2016
LEFT JOIN od_closest pt_any   ON p.gnaf_pid = pt_any.gnaf_pid 
LEFT JOIN od_closest pt_freq  ON p.gnaf_pid = pt_freq.gnaf_pid 
LEFT JOIN (SELECT p.gnaf_pid, min(distance) AS distance
             FROM parcel_dwellings p
             LEFT JOIN 
             (SELECT gnaf_pid,
                    (obj->>'aos_id')::int AS aos_id,
                    (obj->>'distance')::int AS distance
              FROM od_aos_jsonb,
                   jsonb_array_elements(attributes) obj) o ON p.gnaf_pid = o.gnaf_pid
             LEFT JOIN open_space_areas pos ON o.aos_id = pos.aos_id
                 WHERE pos.aos_id IS NOT NULL
                   AND aos_ha_public > 0
             GROUP BY p.gnaf_pid) pos_any ON p.gnaf_pid = pos_any.gnaf_pid
LEFT JOIN (SELECT p.gnaf_pid,  min(distance) AS distance
             FROM parcel_dwellings p
             LEFT JOIN 
             (SELECT gnaf_pid,
                    (obj->>'aos_id')::int AS aos_id,
                    (obj->>'distance')::int AS distance
              FROM od_aos_jsonb,
                   jsonb_array_elements(attributes) obj) o ON p.gnaf_pid = o.gnaf_pid
             LEFT JOIN open_space_areas pos ON o.aos_id = pos.aos_id
                 WHERE pos.aos_id IS NOT NULL
                   AND aos_ha_public > 1.5
             GROUP BY p.gnaf_pid) pos_large ON p.gnaf_pid = pos_large.gnaf_pid
WHERE NOT EXISTS (SELECT 1 
                    FROM excluded_parcels x 
                   WHERE x.gnaf_pid = p.gnaf_pid)
     AND pt_any.dest_class ='gtfs_2018_stops'
     AND pt_freq.dest_class ='gtfs_2018_stop_30_mins_final'
GROUP BY mb_code_2016, dwelling, person;
''' 
 
print("Create NCPF mesh block results... "),
curs.execute(create_ncpf_mb_indicators)
conn.commit()
print("Done.")

create_ncpf_region_summary = '''
DROP TABLE IF EXISTS ncpf_region3;
CREATE TABLE ncpf_region3 AS
SELECT SUM(dwelling)::int AS dwellings,
	   SUM(person)::int   AS persons,
	   ROUND((100*SUM(dw_w_pt_any   )/SUM(dwelling))::numeric, 2) AS pct_pt_any   ,
	   ROUND((100*SUM(dw_w_pt_freq  )/SUM(dwelling))::numeric, 2) AS pct_pt_freq  ,
	   ROUND((100*SUM(dw_w_pos_any  )/SUM(dwelling))::numeric, 2) AS pct_pos_any  ,
	   ROUND((100*SUM(dw_w_pos_large)/SUM(dwelling))::numeric, 2) AS pct_pos_large
FROM ncpf_mb3;
'''
print("Create NCPF region results... "),
curs.execute(create_ncpf_region_summary)
conn.commit()
print("Done.")

summary = pandas.read_sql_query('''SELECT * FROM ncpf_region3''',con=engine)
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
