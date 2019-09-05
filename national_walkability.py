import time
import psycopg2 
import numpy as np
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

locale = 'australia'
db = 'li_australia_2018'

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()




engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))
print("Calculate parcel level national walkability index... "),
sql = '''
-- Create national walkability index
-- DROP TABLE IF EXISTS wa_1600m_ntnl;
CREATE TABLE IF NOT EXISTS wa_1600m_ntnl AS
SELECT gnaf_pid,
       wa_dns_1600m_dd_2018,
       wa_dns_1600m_sc_2018,
       wa_sco_1600m_dl_2018,
       z_wa_dns_1600m_dd_2018,
       z_wa_dns_1600m_sc_2018,
       z_wa_sco_1600m_dl_2018,
       (z_wa_sco_1600m_dl_2018 + z_wa_dns_1600m_sc_2018 + z_wa_dns_1600m_dd_2018) AS wa_sco_1600m_national_2018
FROM (SELECT gnaf_pid, 
             walk_18      AS wa_dns_1600m_dd_2018,
             walk_19      AS wa_dns_1600m_sc_2018,
             walk_20_soft AS wa_sco_1600m_dl_2018,
             (walk_18      - AVG(walk_18)      OVER())/stddev_pop(walk_18)      OVER() as z_wa_dns_1600m_dd_2018,
             (walk_19      - AVG(walk_19)      OVER())/stddev_pop(walk_19)      OVER() as z_wa_dns_1600m_sc_2018,
             (walk_20_soft - AVG(walk_20_soft) OVER())/stddev_pop(walk_20_soft) OVER() as z_wa_sco_1600m_dl_2018 
      FROM parcel_indicators
      WHERE exclude IS NULL) t;
CREATE UNIQUE INDEX IF NOT EXISTS ix_wa_1600m_ntnl ON  wa_1600m_ntnl (gnaf_pid);  
'''
curs.execute(sql)
conn.commit()
print("Done.")

print("Create mesh block level walkability aggregation... "),
sql = '''
    DROP TABLE IF EXISTS wa_1600m_ntnl_mb;
    CREATE TABLE IF NOT EXISTS wa_1600m_ntnl_mb AS
    SELECT a.mb_code_2016,
           a.dwelling,
           a.person,
           COUNT(w.*) AS sample_count,
           AVG("wa_sco_1600m_national_2018") AS avg_wa_sco_1600m_national_2018,
           STDDEV_SAMP("wa_sco_1600m_national_2018") AS sd_wa_sco_1600m_national_2018,
           percentile_cont(ARRAY[0,0.01,0.025,0.25,0.5,0.75,0.975,0.99,1]) 
             WITHIN GROUP (ORDER BY "wa_sco_1600m_national_2018") AS percentiles
    FROM area_linkage a
    LEFT JOIN parcel_indicators p USING (mb_code_2016)
    LEFT JOIN wa_1600m_ntnl w ON p.gnaf_pid = w.gnaf_pid
    WHERE a.irsd_score IS NOT NULL
      AND a.dwelling > 0
      AND a.urban = 'urban'
      AND a.study_region IS TRUE
      AND w.gnaf_pid IS NOT NULL
      GROUP BY a.mb_code_2016,a.dwelling, a.person;
    CREATE UNIQUE INDEX IF NOT EXISTS ix_wa_1600m_ntnl_mb ON  wa_1600m_ntnl_mb (mb_code_2016);  
'''
curs.execute(sql)
conn.commit()
print("Done.")

for area in analysis_regions + ['study_region']:  
  if area != 'Mesh Block':
    print("Create {} level walkability aggregation... ".format(area)),
    if area != 'study_region':
        area_id = df_regions.loc[area,'id']
        abbrev = df_regions.loc[area,'abbreviation']
        linkage = 'LEFT JOIN area_linkage a USING (mb_code_2016)'
    else:
        area_id = 'study_region'
        abbrev = 'region'
        linkage = 'LEFT JOIN (SELECT DISTINCT ON (mb_code_2016) mb_code_2016, study_region FROM parcel_indicators) a USING (mb_code_2016)'
    sql = '''
    -- Create national walkability index
    DROP TABLE IF EXISTS wa_1600m_ntnl_{area};
    CREATE TABLE IF NOT EXISTS wa_1600m_ntnl_{area} AS
    SELECT {area_code},
           SUM(w.dwelling) AS dwelling,
           SUM(w.person)  AS person,
           (SUM(w.dwelling* avg_wa_sco_1600m_national_2018)/SUM(w.dwelling))::numeric AS avg_wa_sco_1600m_national_2018_dwelling,
           (SUM(w.person* avg_wa_sco_1600m_national_2018)/SUM(w.person))::numeric AS avg_wa_sco_1600m_national_2018_person
    FROM wa_1600m_ntnl_mb w
    {linkage}
    GROUP BY a.{area_code};
    CREATE UNIQUE INDEX IF NOT EXISTS ix_wa_1600m_ntnl_{area} ON   wa_1600m_ntnl_{area} ({area_code});  
    '''.format(area = abbrev,area_code = area_id, linkage = linkage)
    curs.execute(sql)
    conn.commit()
    print("Done.")

# Note: you can check out an individual Mesh Block's contribution to larger area's result using SQL code like:
# SELECT w.* FROM area_linkage LEFT JOIN wa_1600m_ntnl_mb w USING (mb_code_2016) WHERE sa1_maincode_2016 = '10901117227';