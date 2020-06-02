# Purpose: Refresh some database settings following changes in Feb 2019
# Authors:  Carl Higgs
# Date:    20180626
import sys
import psycopg2
import time
import getpass
import os
from script_running_log import script_running_log

start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Refresh some database settings following changes in Feb 2019'

grant_query = '''
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO arc_sde;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO arc_sde;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO python;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO python;
'''

print("\nPlease enter PostgreSQL admin details to grant all privileges to python and arc_sde users") 
admin_user_name = raw_input("Username: ")
admin_pwd = getpass.getpass("Password for user {}: ".format(admin_user_name))
print("Executing grant query and ensuring tablefunc extension is created...")
for region in sys.argv[1:]:
  print("  - {}".format(region))
  db = "li_{}_2018".format(region)
  conn = psycopg2.connect(dbname=db, user=admin_user_name, password=admin_pwd)
  curs = conn.cursor()
  curs.execute(grant_query)
  conn.commit()
  curs.execute('''CREATE EXTENSION IF NOT EXISTS tablefunc;''')
  conn.commit()
  conn.close()

print("\nRenaming script log script names to match current script names in repository... ")
for region in sys.argv[1:]:
  print("  - {}".format(region))
  db = "li_{}_2018".format(region)
  conn = psycopg2.connect(dbname=db, user=admin_user_name, password=admin_pwd)
  curs = conn.cursor()
  sql = '''
  UPDATE script_log SET script = '16_od_distances_closest_in_study_region.py' WHERE script = '15_od_distances_closest_in_study_region.py';
  UPDATE script_log SET script = 'legacy_16_od_count_in_buffer_distance' WHERE script = '16_od_count_in_buffer_distance.py';
  UPDATE script_log SET script = '21_parcel_indicators.py' WHERE script = '20_parcel_indicators.py';
  UPDATE script_log SET script = '20_parcel_exclusion.py' WHERE script = '21_parcel_exclusion.py';
  UPDATE script_log SET script = '21_parcel_indicators.py' WHERE script = '22_parcel_indicators.py';
  '''
  curs.execute(sql)
  conn.commit()
  conn.close()
  
print("\nPurge legacy destinations from existing results tables, in preparation for revised destinations (specifically - alcohol and childcare destinations... ")
for region in sys.argv[1:]:
  print("  - {}".format(region))
  db = "li_{}_2018".format(region)
  conn = psycopg2.connect(dbname=db, user=admin_user_name, password=admin_pwd)
  curs = conn.cursor()
  sql = '''
  DELETE FROM od_closest WHERE dest_class IN ('alcohol_offlicence','alcohol_onlicence','childcare_all','childcare_oshc','childcare_preschool');
  DELETE FROM od_counts WHERE dest_class IN ('alcohol_offlicence','alcohol_onlicence','childcare_all','childcare_oshc','childcare_preschool');
  DELETE FROM log_od_distances WHERE dest_name IN ('alcohol_offlicence_act_2017','alcohol_offlicence_nsw_2017','alcohol_offlicence_nt_2017','alcohol_offlicence_qld_2017','alcohol_offlicence_tas_2018','alcohol_offlicence_vic_2017','alcohol_offlicence_wa_2017','alcohol_onlicence_act_2017','alcohol_onlicence_nsw_2017','alcohol_onlicence_nt_2017','alcohol_onlicence_qld_2017','alcohol_onlicence_tas_2018','alcohol_onlicence_vic_2017','alcohol_onlicence_wa_2017');
  DELETE FROM log_od_counts WHERE dest_name IN ('alcohol_offlicence_act_2017','alcohol_offlicence_nsw_2017','alcohol_offlicence_nt_2017','alcohol_offlicence_qld_2017','alcohol_offlicence_tas_2018','alcohol_offlicence_vic_2017','alcohol_offlicence_wa_2017','alcohol_onlicence_act_2017','alcohol_onlicence_nsw_2017','alcohol_onlicence_nt_2017','alcohol_onlicence_qld_2017','alcohol_onlicence_tas_2018','alcohol_onlicence_vic_2017','alcohol_onlicence_wa_2017');
  '''
  curs.execute(sql)
  conn.commit()
  conn.close()

# output to completion log    
for region in sys.argv[1:]:
  print("  - {}".format(region))
  locale = region
  conn = psycopg2.connect(dbname=db, user=admin_user_name, password=admin_pwd)
  curs = conn.cursor()
  script_running_log(script, task, start, locale)
  conn.close()


