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
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = "Create list of excluded parcels"

points = sample_point_feature

# INPUT PARAMETERS
# output tables
# In this table {points_id} is not unique --- the idea is that jointly with indicator, {points_id} will be unique; such that we can see which if any parcels are missing multiple indicator values, and we can use this list to determine how many null values each indicator contains (ie. the number of {points_id}s for that indicator)
# The number of excluded parcels can be determined through selection of COUNT(DISTINCT({points_id}))
createTable_exclusions     = '''
  DROP TABLE IF EXISTS ind_point.excluded_parcels;
  CREATE TABLE ind_point.excluded_parcels
  ({points_id} {points_id_type} NOT NULL,
    geom geometry,
    indicator varchar NOT NULL,  
  PRIMARY KEY({points_id},indicator));
  '''.format(points_id=points_id,points_id_type=points_id_type)

insert = "INSERT INTO ind_point.excluded_parcels SELECT a.{points_id},a.geom, ".format(points_id = points_id)
table = "\nFROM {points} AS a \nLEFT JOIN ".format(points=points)
match = " AS b \nON a.{points_id} = b.{points_id}  \nWHERE ".format(points_id=points_id)
null = " IS NULL ON CONFLICT ({points_id},indicator) DO NOTHING ".format(points_id=points_id)

# Island exceptions are defined using ABS constructs in the project configuration file.
# They identify contexts where null indicator values are expected to be legitimate due to true network isolation, 
# not connectivity errors.  
if island_exception not in ['','None']:
  print("\nIsland exception has been defined: {}".format(island_exception))
  island_exception = " a.{points_id} NOT IN (SELECT {points_id} FROM {points} p LEFT JOIN area_linkage s ON p.mb_code_2016 = s.mb_code_2016 WHERE s.{island_exception}) AND ".format(island_exception=island_exception,points_id=points_id,points=points)
  island_reviewed = True
if island_exception =='':
  print("No island exceptions have been noted, but no note has been made in configuration file to indicator this region's network islands have been reviewed.\n If there are no exceptions for this study region, please enter 'None' in the project configuration file or have someone else do this for you.")
  island_reviewed = False
if island_exception == 'None':
  print("An analyst has reviewed this study region and determined that no island exceptions should be made\n(ie. all daily living indicator null values where they arise should lead to exclusion as they imply network connectivity failure)")
  island_exception = ''
  island_reviewed = True
# exclude on null indicator, and on null distance
query = '''
{insert} 'no network buffer'    {table} ind_point.nh1600m {match} b.geom      {null};
{insert} 'null sc_nh1600m'      {table} ind_point.sc_nh1600m         {match} sc_nh1600m  {null};
{insert} 'null dd_nh1600m'      {table} ind_point.dd_nh1600m         {match} dd_nh1600m  {null};
{insert} 'area_ha < 16.5'       {table} ind_point.nh1600m            {match} area_ha < 16.5;
{insert} 'null daily living'    {table} ind_point.ind_daily_living   {match} {island_exception} dl_hard_1600m {null};
{insert} 'not urban parcel_sos' {table} area_linkage ON a.mb_code_2016= area_linkage.mb_code_2016 WHERE sos_name_2016 NOT IN ('Major Urban','Other Urban');
{insert} 'null parcel_sos'      {table} area_linkage ON a.mb_code_2016= area_linkage.mb_code_2016 WHERE sos_name_2016 {null};
{insert} 'no IRSD sa1_maincode' {table} area_linkage ON a.mb_code_2016= area_linkage.mb_code_2016 WHERE irsd_score {null};
'''.format(insert = insert, table = table, match = match, island_exception = island_exception, null = null, points_id=points_id)

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
DROP TABLE IF EXISTS ind_point.excluded_summary_parcels;
CREATE TABLE ind_point.excluded_summary_parcels AS
SELECT DISTINCT ON (x.{points_id})
       x.{points_id},
       p.geom
FROM ind_point.excluded_parcels x
LEFT JOIN {points} p ON x.{points_id} = p.{points_id};

-- Mesh block summary
DROP TABLE IF EXISTS ind_mb.excluded_summary_mb;
CREATE TABLE ind_mb.excluded_summary_mb AS
SELECT
  a.mb_code_2016,
  COUNT(b.{points_id}) AS excluded_parcels,
  COUNT(p.{points_id}) AS total_parcels,
  ROUND(COUNT(b.{points_id})::numeric/COUNT(p.{points_id})::numeric,2)  AS prop_excluded,
  a.mb_category_name_2016,
  a.dwelling             ,
  a.person               ,
  a.area_ha              ,
  a.geom
FROM {points} p
LEFT JOIN area_linkage a on p.mb_code_2016= a.mb_code_2016
LEFT JOIN ind_point.excluded_summary_parcels b on p.{points_id} = b.{points_id}
GROUP BY a.mb_code_2016,
         a.mb_category_name_2016,
         a.dwelling             ,
         a.person               ,
         a.area_ha              ,
         a.geom
ORDER BY a.mb_code_2016;

-- SA1 summary
DROP TABLE IF EXISTS ind_sa1.excluded_summary_sa1;
CREATE TABLE ind_sa1.excluded_summary_sa1 AS
SELECT
  a.sa1_maincode_2016,
  COUNT(b.{points_id}) AS excluded_parcels,
  COUNT(p.{points_id}) AS total_parcels,
  ROUND(COUNT(b.{points_id})::numeric/COUNT(p.{points_id})::numeric,2)  AS prop_excluded,
  SUM(a.dwelling) AS dwelling ,
  SUM(a.person) AS person,
  SUM(a.area_ha),
  s.geom
FROM {points} p
LEFT JOIN area_linkage a on p.mb_code_2016= a.mb_code_2016
LEFT JOIN ind_point.excluded_summary_parcels b on p.{points_id} = b.{points_id}
LEFT JOIN boundaries.sa1_2016_aust s USING (sa1_maincode_2016)
GROUP BY a.sa1_maincode_2016,s.geom
ORDER BY a.sa1_maincode_2016;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO arc_sde;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO arc_sde;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO python;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO python;
'''.format(points_id=points_id,points=points)
print("Create additional summary tables (parcel, mb, sa1) with geometries to explore exclusions spatially... "),
curs.execute(summary_tables)
conn.commit()
print("Done.")

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

try:                                                                 
    print("\nExcluded parcels by reason for exclusion:")
    summary = pandas.read_sql_query('''SELECT indicator, count(*) FROM ind_point.excluded_parcels GROUP BY indicator;''',con=engine) 
    print(summary)
    print("\nExcluded parcels by section of state:")
    summary = pandas.read_sql_query('''SELECT sos_name_2016, COUNT(DISTINCT(a.{points_id})) from parcel_sos a LEFT JOIN ind_point.excluded_parcels b ON a.{points_id} = b.{points_id} WHERE b.{points_id} IS NOT NULL GROUP BY sos_name_2016;'''.format(points_id=points_id),con=engine) 
    print(summary)
    print("\nTotal excluded parcels:")
    summary = pandas.read_sql_query('''SELECT COUNT(DISTINCT({points_id})) FROM ind_point.excluded_parcels'''.format(points_id=points_id),con=engine) 
    print(summary['count'][0])
except:
    print("\nThere appear to have been zero exclusions; however, please verify this manually.")


print("\nNetwork island diagnostics"),
if island_reviewed is False:
  print(" [study region *not yet flagged* as having been reviewed] ")
if island_reviewed is True:
  print(" [study region is flagged as having been reviewed] ")

network_islands = '''
--Create a geometry table for network island clusters 
DROP TABLE IF EXISTS validation.network_islands; 
CREATE TABLE validation.network_islands AS 
SELECT ST_Length(geom) AS length,  
       geom 
FROM (SELECT ST_SetSRID( 
           ST_CollectionHomogenize( 
             unnest(  
               ST_ClusterIntersecting( 
                 geom 
               ) 
             ) 
           ), 
           7845 
         ) AS geom FROM network.edges) t; 
         
'''
curs.execute(network_islands)
conn.commit()
curs.execute(grant_query)
conn.commit()

print("(check table 'network_islands' to see if any large non-main network islands are legitimate islands;\nif so, they can be whitelisted in the project configuration file)\nSummary of network islands:")

# summary = pandas.read_sql_query('''
# --Summarise length in descending order 
# SELECT ROUND(length::numeric,0)::int AS length_metres from network_islands ORDER BY length DESC;  
# ''',con=engine) 
# print(summary)

print('')
# output to completion log    
script_running_log(script, task, start, locale)

# clean up
conn.close()
