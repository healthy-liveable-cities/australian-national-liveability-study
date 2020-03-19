


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
task = 'create destination indicator tables'

db = 'li_syd_2018'

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

subset_location = 'western_sydney'
subset_list = "'Blue Mountains (C)','Camden (A)','Campbelltown (C) (NSW)','Fairfield (C)','Hawkesbury (C)','Liverpool (C)','Penrith (C)','Wollondilly (A)'"

print('''
Preparing National Cities Performance Framework summary for {subset_location}, a subset of {locale}.
'''.format(subset_location = subset_location, locale = locale))

# connect to postgresql database    
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

area = 'study region'
area_id = 'study_region'
abbrev  = 'region'
include_region = ''
pkey = area_id
study_region = 'Western Sydney'
housing_diversity = ''
housing_diversity_join = ''
for standard in ['dwelling','person']:
        print("  - li_inds_{}_{}".format(abbrev,standard))
        sql = '''
        DROP TABLE IF EXISTS li_inds_{abbrev}_{standard}_{subset_location};
        CREATE TABLE li_inds_{abbrev}_{standard}_{subset_location} AS
        SELECT t.*,
               {housing_diversity}
               g.dwelling gross_dwelling,
               g.area gross_area,
               g.gross_density,
               g.urban_dwelling urban_gross_dwelling,
               g.urban_area     urban_gross_area,
               g. urban_gross_density,
               g.not_urban_dwelling not_urban_gross_dwelling,
               g.not_urban_area     not_urban_gross_area,
               g.not_urban_gross_density,
               n.dwelling net_dwelling,
               n.area net_area,
               n.net_density,
               n.urban_dwelling urban_net_dwelling,
               n.urban_area     urban_net_area,
               n.urban_net_density,
               n.not_urban_dwelling not_urban_net_dwelling,
               n.not_urban_area     not_urban_net_area,
               n.not_urban_net_density
        FROM
        (SELECT 
         {study_region} AS study_region,
         {subset_location} AS locale,
         SUM(dwelling) AS dwelling,
         SUM(person) AS person,
         SUM(sample_count) AS sample_count,
         SUM(sample_count)/SUM(area_ha) AS sample_count_per_ha,
         SUM(area_ha) AS area_ha,
         {extract},
         ST_Union(geom) AS geom
         FROM area_indicators_mb_json,
              jsonb_array_elements(indicators) ind
         ) t
        LEFT JOIN abs_density_gross_{abbrev} g USING ({area_id})
        LEFT JOIN abs_density_net_{abbrev} n USING ({area_id})
        {housing_diversity_join}
        WHERE lga_name_2016 IN ({subset});
        '''.format(area_id = area_id,
                   abbrev = abbrev,
                   include_region = include_region,
                   extract = ','.join(['''
                       (CASE             
                            -- if there are no units (dwellings or persons) the indicator is null
                            WHEN COALESCE(SUM({standard}),0) = 0
                                THEN NULL
                            -- else, calculate the value of the unit weighted indicator
                            ELSE                             
                               (SUM({standard}*((ind->'{i}')->>'mean')::numeric)/SUM({standard}))::numeric
                          END) AS "{i}"
                   '''.format(i = i,standard = standard) for i in ind_list]),
                   standard = standard,
                   subset = subset,
                   locale = locale,
                   subset_location = subset_location,
                   housing_diversity = housing_diversity,
                   housing_diversity_join = housing_diversity_join
                   )
        curs.execute(sql)
        conn.commit()
        sql = '''
        ALTER TABLE  li_inds_{abbrev}_{standard}_{subset_location} ADD PRIMARY KEY ({pkey});
        '''.format(pkey = pkey,
                   abbrev = abbrev,
                   standard = standard)
        curs.execute(sql)
        conn.commit()

summary = pandas.read_sql_query('''SELECT study_region, dwelling, person, trans_04_hard, trans_08, trans_09 FROM li_inds_region_dwelling_{subset_location}'''.format(subset_location = subset_location),con=engine)
print(summary)

