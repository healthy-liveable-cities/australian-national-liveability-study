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

print("Please enter default PostgreSQL database details to procede with new database creation, or close terminal to abort.")
admin_db   = input("Database: ")    
admin_user = input("Username: ")
admin_pwd = getpass.getpass("Password for user {} on database {}: ".format(admin_user, admin_db))

print("Connecting to default database to action queries.")
engine = create_engine(f"postgresql://{admin_user}:{admin_pwd}@{db_host}/{admin_db}",
                       isolation_level='AUTOCOMMIT')
conn = engine.connect()

print(f'Creating database if not exists {db}... '),
result = engine.execute(f'''SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db}' ''').fetchone()
if result is None:
    conn.execute(f'''
        CREATE DATABASE {db}
        WITH OWNER = {admin_user} 
        ENCODING = 'UTF8' 
        LC_COLLATE = 'English_Australia.1252' 
        LC_CTYPE = 'English_Australia.1252' 
        TABLESPACE = pg_default 
        CONNECTION LIMIT = -1
        TEMPLATE template0;
        ''') 
    conn.execute(f'''COMMENT ON DATABASE {db} IS '{dbComment}'; ''')
    print('Done.')
else:
    print(f"Database {db} already exists...")


for user in [db_user,arc_sde_user]:
    print(f'Creating user {user}  if not exists... '),
    conn.execute(f'''
      DO
      $do$
      BEGIN
         IF NOT EXISTS (
            SELECT   
            FROM   pg_catalog.pg_roles
            WHERE  rolname = '{user}') THEN
            CREATE ROLE {user} 
            LOGIN
            CONNECTION LIMIT -1 
            PASSWORD '{db_pwd}';
         END IF;
      END
      $do$;
      ''')
    print('Done.')

conn.close()

print(f"Connecting to {db}.")
engine = create_engine(f"postgresql://{admin_user}:{admin_pwd}@{db_host}/{db}", isolation_level='AUTOCOMMIT')
conn = engine.connect()

print('Creating required extensions ... '),
sql = '''
  CREATE EXTENSION IF NOT EXISTS postgis; 
  CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;
  SELECT postgis_full_version(); 
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

print('Creating schemas...')
for schema in schemas:
    print(f'   - {schema}')
    engine.execute(f'''CREATE SCHEMA IF NOT EXISTS {schema};''')
    
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
