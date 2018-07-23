# Script:  18_parcel_exclusion.py
# Purpose: This script develops a list of suspect parcels to investigate and exclude.
# Author:  Carl Higgs

import os
import sys
import time
import psycopg2

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
{insert} 'sc_nh1600m'                   {table} sc_nh1600m       {attribute} sc_nh1600m {null};
{insert} 'dd_nh1600m'                   {table} dd_nh1600m       {attribute} dd_nh1600m {null};
{insert} 'daily living'                 {table} ind_daily_living {attribute} dl_hard    {null};
{insert} 'parcel_sos'                   {table} parcel_sos       {attribute} sos_name_2016 NOT IN ('Major Urban','Other Urban');
{insert} 'sa1_maincode'                 {table} abs_linkage ON a.mb_code_20 = abs_linkage.mb_code_2016 
    WHERE abs_linkage.sa1_maincode NOT IN (SELECT sa1_maincode FROM abs_2016_irsd)
    ON CONFLICT ({id},indicator) DO NOTHING;
'''.format(insert = qA, table = qB, attribute = qC, null = qD, id = points_id.lower())

### NOTE - need to incorporate POS exclusions!!!!!


# OUTPUT PROCESS
print("{} for {}:".format(task,locale))

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

curs.execute(createTable_exclusions)
conn.commit()

curs.execute(query)
conn.commit()

print("To view how many excluded parcels you have by section of state, run this query in psql:")
print("SELECT sos_name_2016, COUNT(DISTINCT(a.gnaf_pid)) from parcel_sos a LEFT JOIN excluded_parcels b ON a.gnaf_pid = b.gnaf_pid WHERE b.gnaf_pid IS NOT NULL GROUP BY sos_name_2016;")

# output to completion log    
script_running_log(script, task, start, locale)

# clean up
conn.close()
 
 
 
 
 