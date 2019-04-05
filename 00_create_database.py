# Script:  00_create_database.py
# Purpose: Facilitate creation of a PostgreSQL database 
# Authors: Carl Higgs
# Context: Used to create database and related settings for creation of liveability indicators

import psycopg2
import time
import getpass
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create region-specific liveability indicator database and user'

# Create study region folder
if not os.path.exists(os.path.join(folderPath,'study_region')):
    os.makedirs(os.path.join(folderPath,'study_region'))
if not os.path.exists(locale_dir):
    os.makedirs(locale_dir)    

# INPUT PARAMETERS
# default database
print("Please enter default PostgreSQL database details to procede with new database creation, or close terminal to abort.")
admin_db   = raw_input("Database: ")    
admin_user_name = raw_input("Username: ")
admin_pwd = getpass.getpass("Password for user {} on database {}: ".format(admin_user_name, admin_db))

# SQL queries
createDB = '''
-- Create database
CREATE DATABASE {db}
WITH OWNER = {admin_user_name} 
ENCODING = 'UTF8' 
LC_COLLATE = 'English_Australia.1252' 
LC_CTYPE = 'English_Australia.1252' 
TABLESPACE = pg_default 
CONNECTION LIMIT = -1
TEMPLATE template0;
COMMENT ON DATABASE {db} IS '{dbComment}';

-- Create user
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT   
      FROM   pg_catalog.pg_roles
      WHERE  rolname = '{db_user}') THEN
      
      CREATE ROLE {db_user} LOGIN PASSWORD '{db_pwd}';
   END IF;
END
$do$;
'''.format(db = db,
           admin_user_name = admin_user_name,
           dbComment = dbComment,
           db_user = db_user,
           db_pwd = db_pwd)  

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

print("Connecting to {}.".format(db))
conn = psycopg2.connect(dbname=db, user=admin_user_name, password=admin_pwd)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
curs = conn.cursor()
 
print('Creating required extensions ... '), 
create_extensions = '''
CREATE EXTENSION IF NOT EXISTS postgis; 
CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;
SELECT postgis_full_version(); 
CREATE EXTENSION IF NOT EXISTS hstore; 
CREATE EXTENSION IF NOT EXISTS tablefunc;
'''
curs.execute(create_extensions)
print('Done.')

print('Creating threshold functions ... '),
create_threshold_functions = '''
CREATE OR REPLACE FUNCTION threshold_hard(in int, in int, out int) 
RETURNS NULL ON NULL INPUT
AS $$ SELECT ($1 < $2)::int $$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION threshold_soft(in int, in int, out double precision) 
RETURNS NULL ON NULL INPUT
AS $$ SELECT 1 - 1/(1+exp(-{slope}*($1-$2)/($2::float))) $$
LANGUAGE SQL;    
'''.format(slope = soft_threshold_slope)
curs.execute(create_threshold_functions)
print('Done.')

# output to completion log		
from script_running_log import script_running_log			
script_running_log(script, task, start, locale)
conn.close()
