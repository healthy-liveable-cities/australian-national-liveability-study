# Script:  18_parcel_exclusion.py
# Purpose: This script develops a list of suspect parcels to investigate and exclude.
# Author:  Carl Higgs

import os
import sys
import time
import psycopg2
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = "Create list of excluded parcels"


# INPUT PARAMETERS
# output tables
# In this table {id} is not unique --- the idea is that jointly with indicator, {id} will be unique; such that we can see which if any parcels are missing multiple indicator values, and we can use this list to determine how many null values each indicator contains (ie. the number of {id}s for that indicator)
# The number of excluded parcels can be determined through selection of COUNT(DISTINCT({id}))
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
{insert} 'no network buffer'    {table} sausagebuffer_1600 {attribute} b.geom {null};
{insert} 'null sc_nh1600m'      {table} sc_nh1600m         {attribute} sc_nh1600m {null};
{insert} 'null dd_nh1600m'      {table} dd_nh1600m         {attribute} dd_nh1600m {null};
{insert} 'null daily living'    {table} ind_daily_living   {attribute} dl_hyb_hard    {null};
{insert} 'not urban parcel_sos' {table} parcel_sos         {attribute} sos_name_2016 NOT IN ('Major Urban','Other Urban');
{insert} 'no IRSD sa1_maincode' {table} abs_linkage ON a.mb_code_20 = abs_linkage.mb_code_2016 
    WHERE abs_linkage.sa1_maincode NOT IN (SELECT sa1_maincode FROM abs_2016_irsd)
    ON CONFLICT ({id},indicator) DO NOTHING;
'''.format(insert = qA, table = qB, attribute = qC, null = qD, id = points_id.lower())

# OUTPUT PROCESS
print("\n{} for {}...".format(task,locale)),

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

curs.execute(createTable_exclusions)
conn.commit()

curs.execute(query)
conn.commit()
print("Done.")

summary_tables = '''
-- parcel summary
DROP TABLE IF EXISTS excluded_summary_parcels;
CREATE TABLE excluded_summary_parcels AS
SELECT gnaf_pid,
       geom
FROM parcel_dwellings
WHERE gnaf_pid IN (SELECT DISTINCT(gnaf_pid) gnaf_pid FROM excluded_parcels);

-- Mesh block summary
DROP TABLE IF EXISTS excluded_summary_mb;
CREATE TABLE excluded_summary_mb AS
SELECT
  p.mb_code_20 AS mb_code_2016,
  COUNT(b.gnaf_pid) AS excluded_parcels,
  COUNT(p.gnaf_pid) AS total_parcels,
  ROUND(COUNT(b.gnaf_pid)::numeric/COUNT(p.gnaf_pid)::numeric,2)  AS prop_excluded,
  a.mb_category_name_2016,
  a.dwelling             ,
  a.person               ,
  a.area_ha              ,
  a.geom
FROM parcel_dwellings p
LEFT JOIN abs_linkage a on p.mb_code_20 = a.mb_code_2016
LEFT JOIN excluded_summary_parcels b on p.gnaf_pid = b.gnaf_pid
GROUP BY p.mb_code_20,
         a.mb_category_name_2016,
         a.dwelling             ,
         a.person               ,
         a.area_ha              ,
         a.geom
ORDER BY p.mb_code_20;

-- SA1 summary
DROP TABLE IF EXISTS excluded_summary_sa1;
CREATE TABLE excluded_summary_sa1 AS
SELECT
  a.sa1_maincode,
  COUNT(b.gnaf_pid) AS excluded_parcels,
  COUNT(p.gnaf_pid) AS total_parcels,
  ROUND(COUNT(b.gnaf_pid)::numeric/COUNT(p.gnaf_pid)::numeric,2)  AS prop_excluded,
  SUM(a.dwelling) AS dwelling ,
  SUM(a.person) AS person,
  SUM(a.area_ha),
  s.geom
FROM parcel_dwellings p
LEFT JOIN abs_linkage a on p.mb_code_20 = a.mb_code_2016
LEFT JOIN excluded_summary_parcels b on p.gnaf_pid = b.gnaf_pid
LEFT JOIN main_sa1_2016_aust_full s ON a.sa1_maincode = s.sa1_mainco
GROUP BY a.sa1_maincode,s.geom
ORDER BY a.sa1_maincode;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO arc_sde;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO arc_sde;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO python;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO python;
'''
print("Create additional summary tables (parcel, mb, sa1) with geometries to explore exclusions spatially... "),
curs.execute(summary_tables)
conn.commit()
print("Done.")

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))
                                                                 
print("\nExcluded parcels by reason for exclusion:")
summary = pandas.read_sql_query('''SELECT indicator, count(*) FROM excluded_parcels GROUP BY indicator;''',con=engine) 
print(summary)
                                                                 
print("\nExcluded parcels by section of state:")
summary = pandas.read_sql_query('''SELECT sos_name_2016, COUNT(DISTINCT(a.gnaf_pid)) from parcel_sos a LEFT JOIN excluded_parcels b ON a.gnaf_pid = b.gnaf_pid WHERE b.gnaf_pid IS NOT NULL GROUP BY sos_name_2016;''',con=engine) 
print(summary)
print('')
# output to completion log    
script_running_log(script, task, start, locale)

# clean up
conn.close()
