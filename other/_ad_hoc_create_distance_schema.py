# Script:  grant_query.py
# Purpose: -- If no argument is given, this script prints out a grant query
#          which may be run manually (
#          ie. to allow python and arc_sde users to modify tables created by
#          the admin user.
#          -- If a study region is specified, admin connection details are 
#          requested and an admin connection is made to that database 
#          in order to execute the query
# Author:  Carl Higgs
# Date:    20190208

# Import custom variables for National Liveability indicator process
import sys
  

if len(sys.argv) < 2:
  print(grant_query)
else:
  import psycopg2
  import time
  import getpass
  import os
  print("Please enter PostgreSQL admin details to grant all privileges to python and arc_sde users")
  admin_user_name = raw_input("Username: ")
  admin_pwd = getpass.getpass("Password for user {}: ".format(admin_user_name))
  print("Executing grant query ...")
  for region in sys.argv[1:]:
    print("  - {}".format(region))
    db = "li_{}_2018".format(region)
    conn = psycopg2.connect(dbname=db, user=admin_user_name, password=admin_pwd)
    curs = conn.cursor()
    # Create schema for distances 
    sql = '''
    CREATE SCHEMA IF NOT EXISTS d_3200m_cl;
    GRANT postgres TO python;
    '''
    curs.execute(sql)
    conn.commit()
    for schema in ['public','d_3200m_cl']:
        for user in ['arc_sde','python']:
            sql = '''
            GRANT USAGE ON SCHEMA {schema} TO {user} ;
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {user};
            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {user};
            GRANT ALL ON ALL TABLES IN SCHEMA {schema} TO {user};
            '''.format(schema=schema, user = user)
            curs.execute(sql)
            conn.commit()
     
  print("Done.")