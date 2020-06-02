# Script:  aedc_national_collation.py
# Purpose: Collate study region indicator output sql dumps
# Author:  Carl Higgs 
# Date:    20190703
# Note:    Assumes the li_australia_2018 database has been created.  
import time
import psycopg2 
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

# Connect to postgresql database     
db = 'li_australia_2018'
year = 2018
exports_dir = os.path.join(folderPath,'study_region','_exports')
print("This script assumes the database {db} has been created!\n".format(db = db))
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()


sql = '''
DROP TABLE IF EXISTS score_card_lga_dwelling    ;
DROP TABLE IF EXISTS score_card_lga_person      ;
DROP TABLE IF EXISTS score_card_mb_dwelling     ;
DROP TABLE IF EXISTS score_card_mb_person       ;
DROP TABLE IF EXISTS score_card_region_dwelling ;
DROP TABLE IF EXISTS score_card_region_person   ;
DROP TABLE IF EXISTS score_card_sa1_dwelling    ;
DROP TABLE IF EXISTS score_card_sa1_person      ;
DROP TABLE IF EXISTS score_card_sa2_dwelling    ;
DROP TABLE IF EXISTS score_card_sa2_person      ;
DROP TABLE IF EXISTS score_card_sa3_dwelling    ;
DROP TABLE IF EXISTS score_card_sa3_person      ;
DROP TABLE IF EXISTS score_card_sa4_dwelling    ;
DROP TABLE IF EXISTS score_card_sa4_person      ;
DROP TABLE IF EXISTS score_card_sos_dwelling    ;
DROP TABLE IF EXISTS score_card_sos_person      ;
DROP TABLE IF EXISTS score_card_ssc_dwelling    ;
DROP TABLE IF EXISTS score_card_ssc_person      ;
DROP TABLE IF EXISTS ind_score_card;
'''
curs.execute(sql)
conn.commit()

print("Create empty tables for parcel indicators... ")
command = 'psql li_australia_2018 < score_cards_schema.sql'
sp.call(command, shell=True,cwd=exports_dir)   

# curs.execute('''SELECT study_region FROM score_card_region_dwelling;''')
# processed_locales = [x[0] for x in curs.fetchall()]
processed_locales = []
print("Done.\n")

print("Looping over study regions and importing data if available and not previously processed...")
locale_field_length = 7 + len(max(study_regions,key=len))
for locale in sorted(study_regions, key=str.lower):
  sql = 'score_card_{}_{}_20191213_Fc.sql'.format(locale,year)
  if locale in processed_locales:
    print((" - {:"+str(locale_field_length)+"}: previously processed").format(locale))
  elif os.path.isfile(os.path.join(exports_dir,sql)):
    print((" - {:"+str(locale_field_length)+"}: processing now... ").format(locale)),
    command = 'pg_restore -a -Fc -d li_australia_2018 < {}'.format(sql)
    sp.call(command, shell=True,cwd=exports_dir)   
    print("Done!")
  else:
    print((" - {:"+str(locale_field_length)+"}: data apparently not available ").format(locale))

print("Create score card report ... "),
sql = '''
DROP TABLE IF EXISTS score_card_national_summary;
CREATE TABLE score_card_national_summary AS
SELECT 
 p.study_region                                                                 ,
 p.dwelling                                                                     ,
 p.person                                                                       ,
 ROUND(p.area_ha                   ::numeric,1) AS  area_ha                     ,
 ROUND(p.daily_living              ::numeric,1) AS  daily_living                ,
 ROUND(p.street_connectivity       ::numeric,0) AS  street_connectivity         ,
 ROUND(p.dwelling_density          ::numeric,0) AS  dwelling_density            ,
 ROUND(p.walkability_index         ::numeric,2) AS  walkability_index           ,
 ROUND(p.liveability_index         ::numeric,2) AS  liveability_index           ,
 ROUND(p.social_infrastructure_mix ::numeric,0) AS  social_infrastructure_mix   ,
 ROUND(p.closest_supermarket       ::numeric,0) AS  closest_supermarket         ,
 ROUND(p.closest_alcohol_offlicence::numeric,0) AS  closest_alcohol_offlicence  ,
 ROUND(p.frequent_pt_400m          ::numeric,2) AS  frequent_pt_400m            ,
 ROUND(p.large_pos_400m            ::numeric,2) AS  large_pos_400m              ,
 ROUND(p.pct_live_work_local_area  ::numeric,2) AS  pct_live_work_local_area    ,
 ROUND(d.pct_30_40_affordable_housing       ,2) AS  pct_30_40_affordable_housing
FROM score_card_region_person p 
LEFT JOIN score_card_region_dwelling d USING (study_region) ;

COPY score_card_national_summary 
  TO 'D:/ntnl_li_2018_template/data/study_region/_exports/score_card_national_summary.csv'
  WITH DELIMITER ','
  CSV HEADER;
'''.format(id = points_id.lower())
curs.execute(sql)
conn.commit()
print("Done.\n")

 # output to completion log    
script_running_log(script, task, start, locale)
conn.close()


