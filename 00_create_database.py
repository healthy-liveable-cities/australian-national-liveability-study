# Script:  0_create_sql_db.py
# Purpose: Facilitate creation of a PostgreSQL database 
# Authors: Carl Higgs, Koen Simons
# Context: Used to create database and related settings for creation of liveability indicators
#
### Successful completion will look something like the following:
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

from sqlalchemy import create_engine
import time
import getpass
import arcpy

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create region-specific liveability indicator database users and ArcSDE connection file'

# Create study region folder
if not os.path.exists(os.path.join(folderPath,'study_region')):
    os.makedirs(os.path.join(folderPath,'study_region'))
if not os.path.exists(locale_dir):
    os.makedirs(locale_dir)    

# INPUT PARAMETERS
# note: these are in general defined in and loaded from _project_setup.py

# default database
print("Please enter default PostgreSQL database details to procede with new database creation, or close terminal to abort.")
admin_db   = raw_input("Database: ")    
admin_user = raw_input("Username: ")
admin_pwd = getpass.getpass("Password for user {} on database {}: ".format(admin_user, admin_db))

              
## OUTPUT PROCESS

print("Connecting to default database to action queries.")
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = admin_user,
                                                                      pwd  = admin_pwd,
                                                                      host = db_host,
                                                                      db   = admin_db),
    isolation_level='AUTOCOMMIT')
conn = engine.connect()
# SQL queries
print('Creating database if not exists {}... '.format(db)),
sql = "SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db}'".format(db = db)
result = engine.execute(sql).fetchone()
if result is None:
    sql = '''
        CREATE DATABASE {db}
        WITH OWNER = {admin_user} 
        ENCODING = 'UTF8' 
        LC_COLLATE = 'English_Australia.1252' 
        LC_CTYPE = 'English_Australia.1252' 
        TABLESPACE = pg_default 
        CONNECTION LIMIT = -1
        TEMPLATE template0;
        '''.format(db = db,
                   admin_user = admin_user,
                   dbComment = dbComment)  
    conn.execute(sql) 
    print('Done.')
else:
    print("Database {} already exists...".format(db))


for user in [db_user,arc_sde_user]:
    print('Creating user {}  if not exists... '.format(user)),
    sql = '''
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
      '''.format(user, db_pwd)  
    conn.execute(sql)
    print('Done.')

conn.close()
# engine.dispose()

print("Connecting to {}.".format(db))
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = admin_user,
                                                                      pwd  = admin_pwd,
                                                                      host = db_host,
                                                                      db   = db),
    isolation_level='AUTOCOMMIT')
conn = engine.connect()
 
print('Creating required extensions ... '),
sql = '''
  CREATE EXTENSION IF NOT EXISTS postgis; 
  CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;
  CREATE EXTENSION IF NOT EXISTS hstore; 
  CREATE EXTENSION IF NOT EXISTS tablefunc;
  '''
conn.execute(sql)
print('Done.')
 
print('Creating threshold functions ... '),
sql = '''
CREATE OR REPLACE FUNCTION threshold_hard(in int, in int, out int) 
    RETURNS NULL ON NULL INPUT
    AS $$ SELECT ($1 < $2)::int $$
    LANGUAGE SQL;

CREATE OR REPLACE FUNCTION threshold_soft(in int, in int, out double precision) 
    RETURNS NULL ON NULL INPUT
    AS $$ SELECT 1 - 1/(1+exp(-5*($1-$2)/($2::float))) $$
    LANGUAGE SQL;    
  '''
engine.execute(sql)
print('Done.')

for schema in schemas:
    print('Creating schema {}... '.format(schema)),
    sql = '''
    CREATE SCHEMA IF NOT EXISTS {};
    '''.format(schema)
    engine.execute(sql)

print('Done.')

# ensure required permissions are granted
engine.execute(grant_query)

if not os.path.isfile(os.path.join(locale_dir,db_sde)):
  print('Creating ArcGIS spatial database connection file ... '),
  arcpy.CreateDatabaseConnection_management(out_folder_path = locale_dir,
                                          out_name = db_sde, 
                                          database_platform = "POSTGRESQL", 
                                          instance = db_host, 
                                          account_authentication = "DATABASE_AUTH", 
                                          username = arc_sde_user, 
                                          password = db_pwd, 
                                          save_user_pass = "SAVE_USERNAME", 
                                          database = db)
  print('Done.')


# output to completion log		
from script_running_log import script_running_log			
script_running_log(script, task, start, locale)
conn.close()
