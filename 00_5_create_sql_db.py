# Script:  0_create_sql_db.py
# Purpose: Facilitate creation of a PostgreSQL database 
# Authors: Carl Higgs, Koen Simons
# Context: Used to create database and related settings for creation of liveability indicators
#
### Successful completion will look like the following:
## Please enter default PostgreSQL database details to procede with new database creation, or close terminal to abort.
## Database: postgres
## Username: postgres
## Password for user postgres on database postgres:
## Connecting to default database to action queries.
## Creating database li_melb_2016...  Done.
## Adding comment "Liveability indicator data for melb 2016."...  Done.
## Creating user python...  Done.
## Creating ArcSDE user arc_sde...  Done.
## Connecting to li_melb_2016.
## Creating PostGIS extension ...  Done.
## Process successfully completed.
## Processing complete (Task: Create region-specific liveability indicator database, users and ArcSDE connection file); duration: 0.29 minutes

import psycopg2
import time
import getpass
import arcpy
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create region-specific liveability indicator database users and ArcSDE connection file'

# INPUT PARAMETERS
# note: these are in general defined in and loaded from config_ntnl_li_process.py

# default database
print("Please enter default PostgreSQL database details to procede with new database creation, or close terminal to abort.")
admin_db   = raw_input("Database: ")    
admin_user_name = raw_input("Username: ")
admin_pwd = getpass.getpass("Password for user {} on database {}: ".format(admin_user_name, admin_db))

# SQL queries
createDB = '''
  CREATE DATABASE {}
  WITH OWNER = {} 
  ENCODING = 'UTF8' 
  LC_COLLATE = 'English_Australia.1252' 
  LC_CTYPE = 'English_Australia.1252' 
  TABLESPACE = pg_default 
  CONNECTION LIMIT = -1
  TEMPLATE template0;
  '''.format(db,admin_user_name)  

commentDB = '''
  COMMENT ON DATABASE {} IS '{}';
  '''.format(db,dbComment)


createUser = '''
  DO
  $do$
  BEGIN
     IF NOT EXISTS (
        SELECT   
        FROM   pg_catalog.pg_roles
        WHERE  rolname = '{0}') THEN
        
        CREATE ROLE {0} LOGIN PASSWORD '{1}';
     END IF;
  END
  $do$;
  '''.format(db_user, db_pwd)  


createUser_ArcSDE = '''
  DO
  $do$
  BEGIN
     IF NOT EXISTS (
        SELECT   
        FROM   pg_catalog.pg_roles
        WHERE  rolname = '{0}') THEN
     CREATE ROLE {0} 
     LOGIN
     NOSUPERUSER
     NOCREATEDB
     NOCREATEROLE
     INHERIT
     NOREPLICATION
     CONNECTION LIMIT -1
     PASSWORD '{1}';
     END IF;
  END
  $do$;
  '''.format(arc_sde_user, db_pwd)  
  
createPostGIS = '''CREATE EXTENSION postgis;SELECT postgis_full_version();'''
  
## OUTPUT PROCESS

print("Connecting to default database to action queries.")
conn = psycopg2.connect(dbname=admin_db, user=admin_user_name, password=admin_pwd)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

print('Creating database {}... '.format(db)),
cur.execute(createDB) 
print('Done.')

print('Adding comment "{}"... '.format(dbComment)),
cur.execute(commentDB)
print('Done.')


print('Creating user {}  if not exists... '.format(db_user)),
cur.execute(createUser)
print('Done.')

print('Creating ArcSDE user {} if not exists... '.format(arc_sde_user)),
cur.execute(createUser_ArcSDE)
print('Done.')  
conn.close()  

print("Connecting to {}.".format(db))
conn = psycopg2.connect(dbname=db, user=admin_user_name, password=admin_pwd)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

print('Creating PostGIS extension ... '),
cur.execute(createPostGIS)
print('Done.')
conn.close()  

print('Creating ArcGIS spatial database connection file ... '),
arcpy.CreateDatabaseConnection_management(out_folder_path = folderPath,
                                          out_name = db_sde, 
                                          database_platform = "POSTGRESQL", 
                                          instance = db_host, 
                                          account_authentication = "DATABASE_AUTH", 
                                          username = arc_sde_user, 
                                          password = db_pwd, 
                                          save_user_pass = "SAVE_USERNAME", 
                                          database = db)
print('Done.')
conn.close()

# output to completion log					
script_running_log(script, task, start, locale)
