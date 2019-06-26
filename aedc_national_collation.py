# Script:  aedc_national_collation.py
# Purpose: Collate study region csv output for aedc measures
# Author:  Carl Higgs 
# Date:    20180717
# Note:    Assumes the li_australia_2018 database has been created.  
import time
import psycopg2 
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

# Connect to postgresql database     
db = 'li_australia_2018'
aedc_dir = os.path.join(folderPath,'study_region','aedc')
print("This script assumes the database {db} has been created!\n".format(db = db))
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

# curs.execute('''SELECT 1 WHERE to_regclass('public.aedc_indicators_aifs') IS NOT NULL;''')
# res = curs.fetchone()
# if res:
    # print("AEDC AIFS table already exists.\n")
    # sql = '''SELECT DISTINCT(locale) FROM aedc_indicators_aifs ORDER BY locale;'''
    # curs.execute(sql)
    # processed_locales = [x[0] for x in curs.fetchall()]
        
# else:
    # print("Create empty AEDC AIFS tables... ")
    # command = 'psql li_australia_2018 < aedc_aifs_schema.sql'
    # sp.call(command, shell=True,cwd=aedc_dir)   
    # processed_locales = []
    # print("Done.\n")

# print("Drop table indices, if existing... "),
# sql = '''
# DROP INDEX IF EXISTS aedc_indicators_aifs_idx;
# DROP INDEX IF EXISTS aedc_indicators_aifs_gix;
# DROP INDEX IF EXISTS aos_acara_naplan_idx    ;
# DROP INDEX IF EXISTS aos_idx                 ;
# DROP INDEX IF EXISTS idx_aos_jsb             ;
# '''.format(id = points_id.lower())
# curs.execute(sql)
# conn.commit()
# print("Done.\n")

# print("Looping over study regions and importing data if available and not previously processed...")
# locale_field_length = 7 + len(max(study_regions,key=len))
# for locale in sorted(study_regions, key=str.lower):
  # sql = 'aedc_aifs_li_{}_2018_Fc.sql'.format(locale)
  # if locale in processed_locales:
    # print((" - {:"+str(locale_field_length)+"}: previously processed").format(locale))
  # elif os.path.isfile(os.path.join(aedc_dir,sql)):
    # print((" - {:"+str(locale_field_length)+"}: processing now... ").format(locale)),
    # command = 'pg_restore -a -Fc -d li_australia_2018 < {}'.format(sql)
    # sp.call(command, shell=True,cwd=aedc_dir)   
    # print("Done!")
  # else:
    # print((" - {:"+str(locale_field_length)+"}: data apparently not available ").format(locale))

# print("\nCreate table indices... "),
# sql = '''
# CREATE UNIQUE INDEX IF NOT EXISTS aedc_indicators_aifs_idx ON aedc_indicators_aifs USING btree ({id});
# CREATE INDEX IF NOT EXISTS aedc_indicators_aifs_gix ON aedc_indicators_aifs USING GIST (geom);
# CREATE UNIQUE INDEX IF NOT EXISTS aos_acara_naplan_idx ON aos_acara_naplan USING btree (aos_id, acara_school_id,locale);
# CREATE UNIQUE INDEX IF NOT EXISTS aos_idx ON open_space_areas USING btree (aos_id,locale);
# CREATE INDEX IF NOT EXISTS idx_aos_jsb ON open_space_areas USING gin (attributes);
# '''.format(id = points_id.lower())
# curs.execute(sql)
# conn.commit()
# print("Done.\n")
 
# print("Create aedc match table... "),
# sql = '''
# DROP TABLE IF EXISTS aedc_aifs_linked;
# CREATE TABLE IF NOT EXISTS aedc_aifs_linked AS
# SELECT
  # aedc.project_id, 
  # latitude, 
  # longitude, 
  # epsg,
  # linkage.*,
  # aedc.geom AS aedc_geom
# FROM aedc_address AS aedc
# CROSS JOIN LATERAL 
  # (SELECT
      # ST_Distance(i.geom, aedc.geom) as match_distance_m,
      # i.*
      # FROM aedc_indicators_aifs i
      # WHERE i.exclude IS NULL
      # AND ST_DWithin(i.geom, aedc.geom, 500) 
      # ORDER BY aedc.geom <-> i.geom
     # LIMIT 1
   # ) AS linkage;
# CREATE INDEX IF NOT EXISTS aedc_participant_idx ON aedc_aifs_linked USING btree (project_id);
# CREATE INDEX IF NOT EXISTS aedc_gnaf_idx ON aedc_aifs_linked USING btree ({id});
# CREATE INDEX IF NOT EXISTS aedc_gnaf_gix ON aedc_aifs_linked USING GIST (geom);
# '''.format(id = points_id.lower())
# curs.execute(sql)
# conn.commit()
# print("Done.\n")
 
print("Create aedc match table... "),
sql = '''
DROP TABLE IF EXISTS aedc_aifs_linked_all_sos;
CREATE TABLE IF NOT EXISTS aedc_aifs_linked_all_sos AS
SELECT
  aedc.project_id, 
  latitude, 
  longitude, 
  epsg,
  linkage.*,
  aedc.geom AS aedc_geom
FROM aedc_address AS aedc
CROSS JOIN LATERAL 
  (SELECT
      ST_Distance(i.geom, aedc.geom) as match_distance_m,
      i.*
      FROM aedc_indicators_aifs i
      WHERE (i.exclude IS NULL OR i.exclude = 'not urban parcel_sos')
      AND ST_DWithin(i.geom, aedc.geom, 500) 
      ORDER BY aedc.geom <-> i.geom
     LIMIT 1
   ) AS linkage;
CREATE INDEX IF NOT EXISTS aedc_participant_idx ON aedc_aifs_linked_all_sos USING btree (project_id);
CREATE INDEX IF NOT EXISTS aedc_gnaf_idx ON aedc_aifs_linked_all_sos USING btree ({id});
CREATE INDEX IF NOT EXISTS aedc_gnaf_gix ON aedc_aifs_linked_all_sos USING GIST (geom);
'''.format(id = points_id.lower())
curs.execute(sql)
conn.commit()
print("Done.\n")
  
print("Output csv with basic measures for additional summary analyses... ")
sql = '''
COPY (
    SELECT project_id                                    ,
           latitude                                      ,
           longitude                                     ,
           epsg                                          ,
           match_distance_m                              ,
           city AS study_region                          ,
           study_region AS sample_region                 ,
           locale                                        ,
           (CASE
                WHEN r.sos_name_2016 IN ('Major Urban','Other Urban') 
                THEN 'urban'
                ELSE 'not urban' 
              END) AS urban_aedc                         ,
           (CASE
                WHEN a.sos_name_2016 IN ('Major Urban','Other Urban') 
                THEN 'urban'
                ELSE 'not urban' 
              END) AS urban_sample                       ,
           sa1_maincode_2016                             ,
           sa2_name_2016                                 ,
           sa3_name_2016                                 ,
           sa4_name_2016                                 ,
           ssc_name_2016                                 ,
           lga_name_2016                                 ,
           gccsa_name_2016                               ,
           state_name_2016                               ,
           r.sos_name_2016                                 ,
           wa_dns_1600m_dd_2018                          ,
           wa_dns_1600m_sc_2018                          ,
           wa_sco_800m_dl_2018                           ,
           wa_sco_1600m_dl_2018                          ,
           wa_sco_800m_ll_2018                           ,
           wa_sco_1600m_ll_2018                          ,
           wa_sco_1600m_wa_2018                          ,
           ed_score_closest_sch_naplan_primary_2018      ,
           ed_avg_800m_sch_naplan_primary_2018           ,
           ed_avg_1600m_sch_naplan_primary_2018          ,
           os_dist_closest_pos_toilet_distance_3200m_2018,
           fo_pct_3200m_healthfood_2018                  ,
           he_dist_closest_gp_nhsd_2017_2018             ,
           he_dist_closest_mch_2018                      ,
           he_dist_closest_hospital_2018                 ,
           he_dist_closest_famsupport_2018               ,
           he_dist_closest_childplay_2018                ,
           he_dist_closest_earlyparent_2018              ,
           he_dist_closest_integrated_2018               ,
           he_dist_closest_pharmacy_nhsd_2017_2018       ,
           he_dist_closest_dentist_nhsd_2017_2018        ,
           he_dist_closest_immunis_2018                  ,
           he_dist_closest_famcounsel_2018               ,
           he_dist_closest_gencounsel_2018               ,
           he_dist_closest_ecintervention_2018           ,
           he_dist_closest_mh_child_adolescent_2018      ,
           he_dist_closest_med_paed_2018                 ,
           he_dist_closest_mh_gen_2018                   ,
           he_dist_closest_mh_adult_2018                 ,
           he_dist_closest_psych_2018
FROM aedc_aifs_linked_all_sos a
LEFT JOIN aedc_address_region r USING(project_id))
TO 'D:/ntnl_li_2018_template/data/study_region/aedc/aedc_aifs_linked_all_sos_simple_stata_20190626.csv' 
WITH CSV DELIMITER ','  HEADER;
'''
curs.execute(sql)
conn.commit()
print("Done.\n")

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()


