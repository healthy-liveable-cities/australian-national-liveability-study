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

import psycopg2
import time
import getpass
import arcpy
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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
  
create_extensions = '''
  CREATE EXTENSION IF NOT EXISTS postgis; 
  CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;
  SELECT postgis_full_version(); 
  CREATE EXTENSION IF NOT EXISTS hstore; 
  CREATE EXTENSION IF NOT EXISTS tablefunc;
  '''

create_threshold_functions = '''
CREATE OR REPLACE FUNCTION threshold_hard(in int, in int, out int) 
    RETURNS NULL ON NULL INPUT
    AS $$ SELECT ($1 < $2)::int $$
    LANGUAGE SQL;

-- a soft threshold indicator (e.g. of access given distance and threshold)
CREATE OR REPLACE FUNCTION threshold_soft(distance int, threshold int) returns float AS 
$$
BEGIN
  -- We check to see if the value we are exponentiation is more or less than 100; if so,
  -- if so the result will be more or less either 1 or 0, respectively. 
  -- If the value we are exponentiating is much > abs(700) then we risk overflow/underflow error
  -- due to the value exceeding the numerical limits of postgresql
  -- If the value we are exponentiating is based on a positive distance, then we know it is invalid!
  -- For reference, a 10km distance with 400m threshold yields a check value of -120, 
  -- the exponent of which is 1.30418087839363e+052 and 1 - 1/(1+exp(-120)) is basically 1 - 1 = 0
  -- Using a check value of -100, the point at which zero is returned with a threshold of 400 
  -- is for distance of 3339km
  IF (distance < 0) 
      THEN RETURN NULL;
  ELSIF (-5*(distance-threshold)/(threshold::float) < -100) 
    THEN RETURN 0;
  ELSE 
    RETURN 1 - 1/(1+exp(-5*(distance-threshold)/(threshold::float)));
  END IF;
END;
$$
LANGUAGE plpgsql
RETURNS NULL ON NULL INPUT;     

 CREATE OR REPLACE FUNCTION threshold_coalesce_hard(in int, in int, out int) 
    AS $$ SELECT COALESCE(($1 < $2)::int,0) $$
    LANGUAGE SQL;

-- a soft threshold indicator (e.g. of access given distance and threshold), coalesce to zero on null input
CREATE OR REPLACE FUNCTION threshold_coalesce_soft(distance int, threshold int) returns float AS 
$$
BEGIN
  -- We check to see if the value we are exponentiation is more or less than 100; if so,
  -- if so the result will be more or less either 1 or 0, respectively. 
  -- If the value we are exponentiating is much > abs(700) then we risk overflow/underflow error
  -- due to the value exceeding the numerical limits of postgresql
  -- If the value we are exponentiating is based on a positive distance, then we know it is invalid!
  -- For reference, a 10km distance with 400m threshold yields a check value of -120, 
  -- the exponent of which is 1.30418087839363e+052 and 1 - 1/(1+exp(-120)) is basically 1 - 1 = 0
  -- Using a check value of -100, the point at which zero is returned with a threshold of 400 
  -- is for distance of 3339km
  --
  -- Because there are instances where nulls may arise because we don't measure to full distance
  -- e.g. for some open space subset queries, we need to allow for these to be legitimate zero, not nulls
  -- Hence, this function returns 0 on Null input
  IF (distance IS NULL) 
      THEN RETURN 0;
  ELSIF (distance < 0) 
      THEN RETURN NULL;
  ELSIF (-5*(distance-threshold)/(threshold::float) < -100) 
    THEN RETURN 0;
  ELSE 
    RETURN 1 - 1/(1+exp(-5*(distance-threshold)/(threshold::float)));
  END IF;
END;
$$
LANGUAGE plpgsql;
  '''      
    
## OUTPUT PROCESS

print("Connecting to default database to action queries.")
conn = psycopg2.connect(dbname=admin_db, user=admin_user_name, password=admin_pwd)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
curs = conn.cursor()

print('Creating database if not exists {}... '.format(db)),
curs.execute("SELECT COUNT(*) = 0 FROM pg_catalog.pg_database WHERE datname = '{}'".format(db))
not_exists_row = curs.fetchone()
not_exists = not_exists_row[0]
if not_exists:
  curs.execute(createDB) 
print('Done.')

print('Adding comment "{}"... '.format(dbComment)),
curs.execute(commentDB)
print('Done.')


print('Creating user {}  if not exists... '.format(db_user)),
curs.execute(createUser)
print('Done.')

print('Creating ArcSDE user {} if not exists... '.format(arc_sde_user)),
curs.execute(createUser_ArcSDE)
print('Done.')  
conn.close()  

print("Connecting to {}.".format(db))
conn = psycopg2.connect(dbname=db, user=admin_user_name, password=admin_pwd)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
curs = conn.cursor()

print('Creating required extensions ... '),
curs.execute(create_extensions)
print('Done.')

print('Creating threshold functions ... '),
curs.execute(create_threshold_functions)
print('Done.')

print('Creating distance results schema {}... '.format(distance_schema)),
sql = '''
CREATE SCHEMA IF NOT EXISTS {};
GRANT postgres TO python;
'''.format(distance_schema)
curs.execute(sql)
for user in ['arc_sde','python']:
    sql = '''
    GRANT USAGE ON SCHEMA {schema} TO {user} ;
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {user};
    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {user};
    GRANT ALL ON ALL TABLES IN SCHEMA {schema} TO {user};
    '''.format(schema=distance_schema, user = user)
    curs.execute(sql)
    conn.commit()
print('Done.')

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
