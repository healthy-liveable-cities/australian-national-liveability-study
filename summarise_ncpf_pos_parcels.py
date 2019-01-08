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
import sys
import os

# Import custom variables for National Liveability indicator process
#from config_ntnl_li_process import *

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

print('Creating TableFunc extension ... ')
for region in sys.argv[1:]:
    print("  - {}".format(region))
    db = "li_{}_2018".format(region)
    conn = psycopg2.connect(dbname=db, user=admin_user_name, password=admin_pwd)
    curs = conn.cursor()
    curs.execute('''CREATE EXTENSION IF NOT EXISTS tablefunc;''')
    conn.commit()

    print("    - Create table of POS NCPF parcel estimates (ind_pos_access_ncpf)... "),
    # Get a list of destinations processed within this region for distance to closest
    sql = '''SELECT DISTINCT(dest_class) FROM od_closest_pos ORDER BY dest_name;'''
    curs.execute(sql)
    categories = [x[0] for x in curs.fetchall()]
    category_list = ','.join(categories)
    category_types = '"{}" int'.format('" int, "'.join(categories))
    crosstab = '''
    DROP TABLE IF EXISTS ind_pos_access_ncpf;
    CREATE TABLE ind_pos_access_ncpf AS
    SELECT p.gnaf_pid, 
           COALESCE(any,0) AS access_pos_any_400m,
           COALESCE(large,0) AS access_pos_large_400m,
           p.geom
    FROM parcel_dwellings p
    LEFT JOIN 
    (SELECT *
      FROM   crosstab(
       'SELECT gnaf_pid, dest_class, ind_hard
        FROM   od_closest_pos
        ORDER  BY 1,2'  -- could also just be "ORDER BY 1" here
      ,$$SELECT unnest('{curly_o}{category_list}{curly_c}'::text[])$$
       ) AS distance ("gnaf_pid" text, {category_types})) t
       ON p.gnaf_pid = t.gnaf_pid;
    --Grant permissions so we can open it in qgis and arcgis 
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO arc_sde; 
    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO arc_sde; 
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO python; 
    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO python;
    '''.format(id = 'gnaf_pid',
               curly_o = "{",
               curly_c = "}",
               category_list = category_list,
               category_types = category_types)
    curs.execute(crosstab)
    conn.commit()
    print("Done.")

print("Done.")	
conn.close()
