# Purpose: Flag parcels for exclusions based on problematic network attributes
# Author:  Carl Higgs

import os
import sys
import time
import psycopg2
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = "Create list of excluded parcels"

print("\n{} for {}...".format(task,locale)),

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

# In this table {id} is not unique --- the idea is that jointly with indicator, {id} will be unique; such that we can see which if any parcels are missing multiple indicator values, and we can use this list to determine how many null values each indicator contains (ie. the number of {id}s for that indicator)
# The number of excluded parcels can be determined through selection of COUNT(DISTINCT({id}))
createTable_exclusions     = '''
  DROP TABLE IF EXISTS excluded_parcels;
  CREATE TABLE excluded_parcels
  ({id} varchar NOT NULL,
    geom geometry,
    indicator varchar NOT NULL,  
  PRIMARY KEY({id},indicator));
  '''.format(id = points_id.lower())
curs.execute(createTable_exclusions)
conn.commit()

# Collate exclusion query

insert = "INSERT INTO excluded_parcels SELECT a.{id},a.geom, ".format(id = points_id.lower())
table = "\nFROM parcel_dwellings AS a \nLEFT JOIN "
match = " AS b \nON a.{id} = b.{id}  \nWHERE ".format(id = points_id.lower())
null = " IS NULL ON CONFLICT ({id},indicator) DO NOTHING ".format(id = points_id.lower())

# exclude on null indicator, and on null distance
query = '''
{insert} 'no network buffer'    {table} sausagebuffer_1600 {match} b.geom      {null};
{insert} 'null sc_nh1600m'      {table} sc_nh1600m         {match} sc_nh1600m  {null};
{insert} 'null dd_nh1600m'      {table} dd_nh1600m         {match} dd_nh1600m  {null};
{insert} 'area_ha < 16.5'       {table} nh1600m            {match} area_ha < 16.5;
{insert} 'not urban parcel_sos' {table} area_linkage ON a.mb_code_2016 = area_linkage.mb_code_2016 WHERE sos_name_2016 NOT IN ('Major Urban','Other Urban');
{insert} 'null parcel_sos'      {table} area_linkage ON a.mb_code_2016 = area_linkage.mb_code_2016 WHERE sos_name_2016 {null};
{insert} 'no IRSD sa1_maincode' {table} area_linkage ON a.mb_code_2016 = area_linkage.mb_code_2016 WHERE irsd_score {null};
'''.format(insert = insert, table = table, match = match, null = null, id = points_id.lower())

curs.execute(query)
conn.commit()
print("Done.")

print('')
# output to completion log    
script_running_log(script, task, start, locale)

# clean up
conn.close()
