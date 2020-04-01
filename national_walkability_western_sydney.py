


# Script:  NCPF_BITRE_summary.py
# Purpose: Create parcel indicators for national liveability project
# Author:  Carl Higgs 
# Date:    20180717

import time
import psycopg2 
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

from datetime import date
today = str(date.today())

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create Western Sydney national walkability score'

db = 'li_australia_2018'

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

subset_location = 'western_sydney'
subset_list = "'Blue Mountains (C)','Camden (A)','Campbelltown (C) (NSW)','Fairfield (C)','Hawkesbury (C)','Liverpool (C)','Penrith (C)','Wollondilly (A)'"

print('''
Preparing National Cities Performance Framework summary for {subset_location}, a subset of Sydney.
'''.format(subset_location = subset_location, locale = locale))

# connect to postgresql database    
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

area = subset_location
area_id = 'study_region'
abbrev  = 'region'
include_region = ''
pkey = area_id
study_region = 'Western Sydney'

area_id = 'study_region'
abbrev = 'region'
linkage = 'LEFT JOIN (SELECT DISTINCT ON (mb_code_2016) mb_code_2016, lga_name_2016 FROM parcel_indicators) a USING (mb_code_2016)'
sql = '''
-- Create national walkability index
DROP TABLE IF EXISTS wa_1600m_ntnl_{area};
CREATE TABLE IF NOT EXISTS wa_1600m_ntnl_{area} AS
SELECT '{study_region}'::text AS study_region,
       SUM(w.dwelling) AS dwelling,
       SUM(w.person)  AS person,
       (SUM(w.dwelling* avg_wa_sco_1600m_national_2018)/SUM(w.dwelling))::numeric AS avg_wa_sco_1600m_national_2018_dwelling,
       (SUM(w.person* avg_wa_sco_1600m_national_2018)/SUM(w.person))::numeric AS avg_wa_sco_1600m_national_2018_person
FROM wa_1600m_ntnl_mb w
{linkage}
WHERE lga_name_2016 IN ({subset})
GROUP BY study_region;
CREATE UNIQUE INDEX IF NOT EXISTS ix_wa_1600m_ntnl_{area} ON   wa_1600m_ntnl_{area} ({area_code});  
'''.format(area = area,area_code = area_id,study_region=study_region, linkage = linkage,subset=subset_list)
curs.execute(sql)
conn.commit()
print("Done.")


summary = pandas.read_sql_query('''SELECT * FROM wa_1600m_ntnl_{subset_location}'''.format(subset_location = subset_location),con=engine)
print(summary)

# COPY (SELECT *,RANK () OVER (ORDER BY avg_wa_sco_1600m_national_2018_dwelling DESC) AS ntnl_wa_dwelling_rank, RANK () OVER (ORDER BY avg_wa_sco_1600m_national_2018_person DESC) AS ntnl_wa_person_rank FROM wa_1600m_ntnl_region UNION wa_1600m_ntnl_western_sydney) TO 'D:/ntnl_wa_20200401.csv' CSV DELIMITER ',' HEADER;
