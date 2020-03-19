# Purpose: basic_li_australia_2018_backup.py
# Author:  Carl Higgs 
# Date:    2020-03-13

import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine
from datetime import datetime

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Export liveability indicator region estimates'

date = datetime.today().strftime('%Y%m%d')
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

db = 'li_australia_2018'
out_dir = 'D:/ntnl_li_2018_template/data/study_region/'
file = 'basic_li_australia_2018_Fc_{}.sql'.format(date)

command = (
   'pg_dump -U {db_user} -h localhost  -Fc '
   ' -t "abs_2016_irsd" '
   ' -t "ind_score_card" '
   ' -t "li_inds_lga_dwelling" '
   ' -t "li_inds_lga_person" '
   ' -t "li_inds_lga_policy" '
   ' -t "li_inds_mb_dwelling" '
   ' -t "li_inds_mb_person" '
   ' -t "li_inds_mb_policy" '
   ' -t "li_inds_region_dwelling" '
   ' -t "li_inds_region_person" '
   ' -t "li_inds_region_policy" '
   ' -t "li_inds_sa1_dwelling" '
   ' -t "li_inds_sa1_person" '
   ' -t "li_inds_sa1_policy" '
   ' -t "li_inds_sa2_dwelling" '
   ' -t "li_inds_sa2_person" '
   ' -t "li_inds_sa2_policy" '
   ' -t "li_inds_sa3_dwelling" '
   ' -t "li_inds_sa3_person" '
   ' -t "li_inds_sa3_policy" '
   ' -t "li_inds_sa4_dwelling" '
   ' -t "li_inds_sa4_person" '
   ' -t "li_inds_sa4_policy" '
   ' -t "li_inds_sos_dwelling" '
   ' -t "li_inds_sos_person" '
   ' -t "li_inds_sos_policy" '
   ' -t "li_inds_ssc_dwelling" '
   ' -t "li_inds_ssc_person" '
   ' -t "li_inds_ssc_policy" '
   ' -t "live_sa1_work_sa3" '
   ' -t "mb_2016_aust" '
   ' -t "od_aos_jsonb" '
   ' -t "open_space_areas" '
   ' -t "osm_20181001_line" '
   ' -t "osm_20181001_point" '
   ' -t "osm_20181001_polygon" '
   ' -t "osm_20181001_roads" '
   ' -t "parcel_indicators" '
   ' -t "sa1_2016_aust" '
   ' -t "sa1_lookup_codes" '
   ' -t "sa1_uli_si_mix_irsd" '
   ' -t "sa2_2016_aust" '
   ' -t "sa3_2016_aust" '
   ' -t "sa4_2016_aust" '
   ' -t "score_card_lga_dwelling" '
   ' -t "score_card_lga_person" '
   ' -t "score_card_mb_dwelling" '
   ' -t "score_card_mb_person" '
   ' -t "score_card_national_summary" '
   ' -t "score_card_region_dwelling" '
   ' -t "score_card_region_person" '
   ' -t "score_card_sa1_dwelling" '
   ' -t "score_card_sa1_person" '
   ' -t "score_card_sa2_dwelling" '
   ' -t "score_card_sa2_person" '
   ' -t "score_card_sa3_dwelling" '
   ' -t "score_card_sa3_person" '
   ' -t "score_card_sa4_dwelling" '
   ' -t "score_card_sa4_person" '
   ' -t "score_card_sos_dwelling" '
   ' -t "score_card_sos_person" '
   ' -t "score_card_ssc_dwelling" '
   ' -t "score_card_ssc_person" '
   ' -t "sos_2016_aust" '
   ' -t "spatial_ref_sys" '
   ' -t "ssc_2016_aust" '
   ' -t "ste_2016_aust" '
   ' -t "study_region" '
   ' -t "study_region_locale" '
   ' -t "study_regions" '
   ' -t "sua_2016_aust" '
   ' -t "ucl_2016_aust" '
   ' -t "wa_1600m_ntnl" '
   ' -t "wa_1600m_ntnl_lga" '
   ' -t "wa_1600m_ntnl_mb" '
   ' -t "wa_1600m_ntnl_region" '
   ' -t "wa_1600m_ntnl_sa1" '
   ' -t "wa_1600m_ntnl_sa2" '
   ' -t "wa_1600m_ntnl_sa3" '
   ' -t "wa_1600m_ntnl_sa4" '
   ' -t "wa_1600m_ntnl_sos" '
   ' -t "wa_1600m_ntnl_ssc" '
   '{db} > {file}'
   ).format(db = db,db_user = db_user,file=file)    
sp.call(command, shell=True,cwd=out_dir)   
print("Done.")