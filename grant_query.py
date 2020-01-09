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
  
distance_schema = 'd_3200m_cl'
db_user = 'python'
arc_sde_user = 'arc_sde'

grant_query = '''
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {db_user};
    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {db_user};
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {arc_sde_user};
    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {arc_sde_user};
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {distance_schema} TO {db_user};
    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {distance_schema} TO {db_user};
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {distance_schema} TO {arc_sde_user};
    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {distance_schema} TO {arc_sde_user};
    '''.format(db_user=db_user, arc_sde_user = arc_sde_user,distance_schema = distance_schema)

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
    curs.execute(grant_query)
    conn.commit()
  print("Done.")