


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

subset_location = 'western_sydney'
subset_list = "'Blue Mountains (C)','Camden (A)','Campbelltown (C) (NSW)','Fairfield (C)','Hawkesbury (C)','Liverpool (C)','Penrith (C)','Wollondilly (A)'"

print('''
Preparing National Cities Performance Framework summary for {subset_location}, a subset of {locale}.
'''.format(subset_location = subset_location, locale = locale))

summarise_parcel_no_results = ['''
SELECT '{region}' AS region, 
       sos_name_2016,
       COUNT(*) AS gtfs_result 
  FROM parcel_sos 
 WHERE gnaf_pid IN (SELECT DISTINCT(o.gnaf_pid) 
                      FROM od_closest o 
                 LEFT JOIN non_abs_linkage s 
                        ON o.gnaf_pid = s.gnaf_pid 
                     WHERE lga_name_2016 IN ({subset})
                       AND dest_name = 'gtfs_2018_stops') 
 GROUP BY sos_name_2016;
'''.format(region = subset_location, subset = subset_list),
'''
SELECT '{region}' AS region, 
       sos_name_2016, 
       COUNT(*) AS gtfs_no_result 
 FROM parcel_sos WHERE gnaf_pid NOT IN (SELECT DISTINCT(o.gnaf_pid) 
                      FROM od_closest o 
                 LEFT JOIN non_abs_linkage s 
                        ON o.gnaf_pid = s.gnaf_pid 
                     WHERE lga_name_2016 IN ({subset})
                       AND dest_name = 'gtfs_2018_stops') 
 GROUP BY sos_name_2016;
'''.format(region = subset_location, subset = subset_list),
'''
SELECT '{region}' AS region, 
       sos_name_2016, 
       COUNT(*) AS pos_result 
  FROM parcel_sos 
 WHERE gnaf_pid IN (SELECT DISTINCT(o.gnaf_pid) 
                      FROM od_aos_jsonb o 
                 LEFT JOIN non_abs_linkage s 
                        ON o.gnaf_pid = s.gnaf_pid 
                     WHERE lga_name_2016 IN ({subset})) 
GROUP BY sos_name_2016;
'''.format(region = subset_location, subset = subset_list),
'''
SELECT '{region}' AS region, 
       sos_name_2016, 
       COUNT(*) AS pos_no_result 
  FROM parcel_sos 
 WHERE gnaf_pid NOT IN (SELECT DISTINCT(o.gnaf_pid) 
                          FROM od_aos_jsonb o 
                     LEFT JOIN non_abs_linkage s 
                            ON o.gnaf_pid = s.gnaf_pid 
                         WHERE lga_name_2016 IN ({subset})) 
GROUP BY sos_name_2016;
'''.format(region = subset_location, subset = subset_list)]                                                                
                                                                 
print("\n Nulls for sample points (GNAF parcels) by indicator and Section of state")
for res in summarise_parcel_no_results:
  summary = pandas.read_sql_query(res,con=engine)
  if len(summary) != 0:
    print(summary)
  if len(summary) == 0:
    print("No result for: {}\n".format(res))

                                                                 
# Define parcel level indicator table creation query
# Note that we modify inds slightly later when aggregated to reflect cutoffs etc

createTable_exclusions     = '''
  DROP TABLE IF EXISTS excluded_parcels;
  CREATE TABLE excluded_parcels
  ({id} varchar NOT NULL,
    indicator varchar NOT NULL,  
  PRIMARY KEY({id},indicator));
  '''.format(id = points_id.lower())

qA = "INSERT INTO excluded_parcels SELECT a.{id}, ".format(id = points_id.lower())
qB = "\nFROM parcel_dwellings AS a \nLEFT JOIN "
qC = " AS b \nON a.{id} = b.{id}  \nWHERE ".format(id = points_id.lower())
qD = " IS NULL ON CONFLICT ({id},indicator) DO NOTHING ".format(id = points_id.lower())
  
# exclude on null indicator, and on null distance
query = '''
{insert} 'no network buffer' {table} sausagebuffer_1600       {attribute} b.geom {null};
{insert} 'parcel_sos'        {table} parcel_sos       {attribute} sos_name_2016 NOT IN ('Major Urban','Other Urban');
{insert} 'sa1_maincode'      {table} abs_linkage ON a.mb_code_20 = abs_linkage.mb_code_2016 
    WHERE abs_linkage.sa1_maincode NOT IN (SELECT sa1_maincode FROM abs_2016_irsd)
    ON CONFLICT ({id},indicator) DO NOTHING;
'''.format(insert = qA, table = qB, attribute = qC, null = qD, id = points_id.lower())

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

print("Create parcel exclusion table... "),
curs.execute(createTable_exclusions)
conn.commit()

curs.execute(query)
conn.commit()
print("Done.")

create_ncpf_mb_indicators = '''
DROP TABLE IF EXISTS ncpf_mb_{subset_location};
CREATE TABLE ncpf_mb_{subset_location} AS
SELECT a.mb_code_2016, 
	   a.dwelling,
	   a.person,
	   a.dwelling * AVG((pt_any.distance < 400)::int) AS dw_w_pt_any,
	   a.dwelling * AVG((pt_freq.distance < 400)::int) AS dw_w_pt_freq,
	   a.dwelling * AVG(any_pos)                    AS dw_w_pos_any,
	   a.dwelling * AVG(large_pos)                  AS dw_w_pos_large
FROM parcel_dwellings p
LEFT JOIN non_abs_linkage s ON p.gnaf_pid = s.gnaf_pid
LEFT JOIN abs_linkage a ON p.mb_code_20 = a.mb_code_2016
LEFT JOIN od_closest pt_any   ON p.gnaf_pid = pt_any.gnaf_pid 
LEFT JOIN od_closest pt_freq  ON p.gnaf_pid = pt_freq.gnaf_pid 
LEFT JOIN (SELECT p.gnaf_pid, COALESCE((min(distance) < 400)::int,0) AS any_pos
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
LEFT JOIN (SELECT p.gnaf_pid, COALESCE((min(distance) < 400)::int,0) AS large_pos
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
WHERE lga_name_2016 IN ({subset}) 
     AND NOT EXISTS (SELECT 1 
                    FROM excluded_parcels x 
                   WHERE x.gnaf_pid = p.gnaf_pid)
     AND pt_any.dest_class ='gtfs_2018_stops'
     AND pt_freq.dest_class ='gtfs_2018_stop_30_mins_final'
GROUP BY mb_code_2016, dwelling, person;
'''.format(subset_location = subset_location, subset = subset_list)                                                              
print("Create NCPF mesh block results... "),
curs.execute(create_ncpf_mb_indicators)
conn.commit()
print("Done.")

create_ncpf_region_summary = '''
DROP TABLE IF EXISTS ncpf_region_{subset_location};
CREATE TABLE ncpf_region_{subset_location} AS
SELECT SUM(dwelling)::int AS dwellings,
	   SUM(person)::int   AS persons,
	   ROUND((100*SUM(dw_w_pt_any   )/SUM(dwelling))::numeric, 2) AS pct_pt_any   ,
	   ROUND((100*SUM(dw_w_pt_freq  )/SUM(dwelling))::numeric, 2) AS pct_pt_freq  ,
	   ROUND((100*SUM(dw_w_pos_any  )/SUM(dwelling))::numeric, 2) AS pct_pos_any  ,
	   ROUND((100*SUM(dw_w_pos_large)/SUM(dwelling))::numeric, 2) AS pct_pos_large
FROM ncpf_mb_{subset_location};
'''.format(subset_location = subset_location)                                                             
print("Create NCPF region results... "),
curs.execute(create_ncpf_region_summary)
conn.commit()
print("Done.")

summary = pandas.read_sql_query('''SELECT * FROM ncpf_region_{subset_location}''',con=engine)
print(summary)
values = [subset_location] + summary.values.tolist()[0]
output = os.path.join(folderPath,'ncpf_region_summary_{}_{}.csv'.format(responsible[locale],today))
header = '''{},{},{},{},{},{},{}\n'''.format('locale','Dwellings','Persons','% PT (any)','% PT (frequent)','% POS (any)','% POS (large)')

if not os.path.exists(output):
   with open(output, "w") as f:
     f.write(header)
with open(output, "a") as f:
  f.write('''{},{},{},{},{},{},{}\n'''.format(*values))
# # output to completion log    
# script_running_log(script, task, start, locale)
# conn.close()
