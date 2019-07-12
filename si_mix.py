# Script:  si_mix.py
# Purpose: Create Social Infrastructure mix score for national liveability project
# Author:  Carl Higgs 
# Date:    20190712

# Note that 'community centres' may need revision, and community health centres were not available

import time
import psycopg2 

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'calculate social infrastructure mix score for {}'.format(locale)

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

print("Updating parcel indicators table with Social Infrastructure mix score... "),
sql  = '''
-- Add a SI mix column if it doesn't already exist to the parcel indicators table
-- and update it with the SI mix values for those parcels
ALTER TABLE parcel_indicators ADD COLUMN IF NOT EXISTS si_mix double precision;
UPDATE parcel_indicators p
   SET si_mix = (COALESCE(threshold_soft("dist_m_community_centre_osm"                  , 1000),0) +
                 COALESCE(threshold_soft("dist_m_museum_osm"                            , 3200),0) +
                 COALESCE(threshold_soft(LEAST("dist_m_cinema_osm","dist_m_theatre_osm"), 3200),0) +
                 COALESCE(threshold_soft("dist_m_libraries_2018"                        , 1000),0) +
                 COALESCE(threshold_soft("dist_m_childcare_oshc_meet_2019"              , 1600),0) +
                 COALESCE(threshold_soft("dist_m_childcare_all_meet_2019"               , 800),0)  +
                 COALESCE(threshold_soft("dist_m_P_12_Schools_gov_2018"                 , 1600),0) +
                 COALESCE(threshold_soft("dist_m_secondary_schools2018"                 , 1600),0) +
                 COALESCE(threshold_soft("dist_m_nhsd_2017_aged_care_residential"       , 1000),0) +
                 COALESCE(threshold_soft("dist_m_nhsd_2017_pharmacy"                    , 1000),0) +
                 COALESCE(threshold_soft("dist_m_nhsd_2017_mc_family_health"            , 1000),0) +
                 COALESCE(threshold_soft("dist_m_nhsd_2017_dentist"                     , 1000),0) +
                 COALESCE(threshold_soft("dist_m_nhsd_2017_gp"                          , 1000),0) +
                 COALESCE(threshold_soft("dist_m_swimming_pool_osm"                     , 1200),0) +
                 COALESCE(threshold_soft("dist_m_swimming_pool_osm"                     , 1200),0) +
                 COALESCE(threshold_soft("sport_distance_m"                        , 1000),0))
                 -- note: did not have community health centres
  FROM dest_closest_indicators d 
  LEFT JOIN ind_os_distance o USING ({id})
  WHERE p.{id} = d.{id};
'''.format(id = points_id)

curs.execute(sql)
conn.commit()
print("Done!")
# output to completion log    
script_running_log(script, task, start)
  

