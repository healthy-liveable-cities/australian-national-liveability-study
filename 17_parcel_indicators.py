# Script:  17_parcel_indicators.py
# Purpose: Create parcel indicators for national liveability project
# Author:  Carl Higgs 
# Date:    20180717

import os
import sys
import time
import psycopg2 
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

ind_matrix = pandas.read_csv(os.path.join(sys.path[0],'ind_study_region_matrix.csv'))
ind_matrix = ind_matrix[[locale,'Description']].dropna()
ind_list = ind_matrix[locale].tolist()

# Define (query,source) tuples for all indicators.  
# These are later appended to respective SQL query string portions based on subset of relevant indicators for a locale,
# and used to build a table containing the linkage codes and indicators for a particular point measurement.
# Some queries may be undefined (e.g. ["",""] ) which is fine --- these will be skipped over.  They can be completed later 
# and the indicator table where such indicators are to be included rebuilt.

# An important formatting point for the indicator queries  - the first item in the tuple - is that they are assumed to end in a comma.  This is because the script regards them as just lists of variables in the SQL table to be created, but not the final one (which is parcel_dwellings.geom).  

## KNIME: % of street blocks with a perimeter of < 720 m (i.e. between 120 m and 240 m long and 60 m and 120 m wide)
walk_1_vic_melb_2016 = ["",""]

## < / >= 80% dwellings < 1 km of an activity centre (with a supermarket?? written, but a necessary condition of activity centre, i think?)
walk_2_vic_melb_2016 = ['''
(CASE WHEN ind_activity.distance <  1000 THEN 1
      WHEN ind_activity.distance >= 1000 THEN 0
      ELSE NULL END) as walk_12_{state}_{locale}_2016_hard,
1-1/(1+exp(-5*(ind_activity.distance-1000)/1000::double precision)) AS walk_12_{state}_{locale}_2016_soft,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN ind_activity ON p.{id} = ind_activity.{id}'''.format(id = points_id)]
## KNIME: </ >= 15 dwellings per net developable hectare
walk_3_vic_melb_2016 = ["",""]

## KNIME: % of street blocks with a perimeter of < 720 m (i.e. <240 m long and <120 m wide)
walk_4_wa_perth_2016 = ["",""]

## NA for moment: % dwellings < 400 m network distance of a secondary or district centre or < 200 m network distance of a neighbourhood centre
walk_5_wa_perth_2016 = ["",""]

## KNIME: < / >= 26 dwellings per net developable hectare
walk_6_wa_perth_2016 = ["",""]

## KNIME: % residential lots by size: <=350m2; >350 - <=550m2; >550 - <=750m2; >750 - <=950m2; >950 m2
walk_7_wa_perth_2016 = ["",""]

## KNIME: % of street blocks with a perimeter of < 560 m (i.e. between 100 m and 200 m long and 40 m and 80 m wide)
walk_8_qld_bris_2016 = ["",""]

## KNIME: < / >= 15 dwellings per hectare for suburban areas
walk_9_qld_bris_2016 = ["",""]

## KNIME: < / >= 30 dwellings per hectare for urban areas ( higher density neighbourhoods typically located around a major centre or secondary centre or large transit node)
walk_10_qld_bris_2016 = ["",""]

## KNIME: < / >= 15 dwellings per hectare for new residential release areas
walk_11_nsw_syd_2016 = ["",""]

## Average distance to closest activity centre, where activity centre is defined as a supermarket < a commercial zoned Mesh Block
walk_12_state_locale_2016 = ['''
ind_activity.distance AS walk_12_{state}_{locale}_2016,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN ind_activity ON p.{id} = ind_activity.{id}'''.format(id = points_id)]
## Bec workflow: average pedshed ratio, defined as the area of properties adjoining the 400 m pedestrian network distance from each residential dwelling divided by the radial "crow flies" area < 400 m; i.e. 50.2 ha.
walk_13_state_locale_2016 = ["",""]
  
## average number of daily living types present, measured as a score of 0-3, with 1 point for each category of (convenience store/petrol station/newsagent, PT stop, supermarket) < 1600m network distance
walk_14_state_locale_2016 = ['''
ind_daily_living.dl_hard AS walk_14_{state}_{locale}_2016_hard,
ind_daily_living.dl_soft AS walk_14_{state}_{locale}_2016_soft,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN ind_daily_living ON p.{id} = ind_daily_living.{id}'''.format(id = points_id)]
## street connectivity, measured as 3+ leg intersection count/area in km2, where the area is computed from the 1600m pedestrian-accessible street network buffered by 50m
walk_15_state_locale_2016 = ['''
sc_nh1600m.sc_nh1600m AS walk_15_{state}_{locale}_2016,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN sc_nh1600m ON p.{id} = sc_nh1600m.{id}'''.format(id = points_id)]
## dwelling density, calculated as the aggregate sum of dwellings recorded in the meshblock polygons intersecting the 1600m pedestrian-accessible street network once buffered by 50m ("neighbourhood") and divided by the "neighbourhood" area in hectares.
walk_16_state_locale_2016 = ['''
dd_nh1600m.dd_nh1600m AS walk_16_{state}_{locale}_2016,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN dd_nh1600m ON p.{id} = dd_nh1600m.{id}'''.format(id = points_id)]
## composite walkability index, combining street connectivity, dwelling density and daily living destinations
walk_17_state_locale_2016 = ['''
ind_walkability.wa_hard AS walk_17_{state}_{locale}_2016_hard,
ind_walkability.wa_soft AS walk_17_{state}_{locale}_2016_soft,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN ind_walkability ON p.{id} = ind_walkability.{id}'''.format(id = points_id)]
## < / >= 95% of of residential cadastre with access to bus stop < 400 m OR < 600m of a tram stop OR < 800 m of a train station
trans_1_vic_melb_2016 = ['''
(CASE WHEN SUM(vic_pt.ind_hard_coalesced) > 0 THEN 1 ELSE 0 END) AS trans_1_vic_melb_2016_hard,
MAX(vic_pt.ind_soft_coalesced) AS trans_1_vic_melb_2016_soft,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(ind_hard,0) AS ind_hard_coalesced, COALESCE(ind_soft,0) AS ind_soft_coalesced FROM parcel_dwellings LEFT JOIN od_closest ON parcel_dwellings.{id} = od_closest.{id} AND  dest IN (9,10,11)) vic_pt ON p.{id} = vic_pt.{id}'''.format(id = points_id)]

## < / >= 95% of dwellings with access to bus stop < 400 m
trans_2_vic_melb_2016 = ['''
vic_bus.ind_hard AS trans_2_vic_melb_2016_hard,
vic_bus.ind_soft AS trans_2_vic_melb_2016_soft,''',
'''LEFT JOIN (SELECT * FROM od_closest WHERE dest = 9) vic_bus ON p.{id} = vic_bus.{id}'''.format(id = points_id)]

## NA for moment: < / >= 60% of residential cadastre < 400m walk from a neighbourhood or town centre, or a bus stop, or in a 800m walk from a railway station.
trans_3_wa_perth_2016 = ["",""]

## < / >= 100% of residential cadastre < 400 metres of an existing or planned public transport stop
trans_4_qld_bris_2016 = ['''
(CASE WHEN qld_pt.distance <  400  THEN 1
      WHEN qld_pt.distance >= 400 THEN 0
      ELSE NULL END) as trans_4_qld_bris_2016_hard,
 1-1/(1+exp(-5*(qld_pt.distance-400)/400::double precision)) AS trans_4_qld_bris_2016_soft,''',
'''LEFT JOIN (SELECT * FROM od_closest WHERE dest = 8) qld_pt ON p.{id} = qld_pt.{id}'''.format(id = points_id)]
## < / >= 100% of residential cadastre < 400 m of a bus stop every 30 min OR < 800 m of a train station every 15 min
trans_5_nsw_syd_2016 = ['''
(CASE WHEN SUM(nsw_pt.ind_hard_coalesced) > 0 THEN 1 ELSE 0 END) AS trans_5_nsw_syd_2016_hard,
MAX(nsw_pt.ind_soft_coalesced) AS trans_5_nsw_syd_2016_soft,''',
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(ind_hard,0) AS ind_hard_coalesced, COALESCE(ind_soft,0) AS ind_soft_coalesced FROM parcel_dwellings LEFT JOIN od_closest ON parcel_dwellings.{id} = od_closest.{id} AND dest IN (14,15)) nsw_pt ON p.{id} = nsw_pt.{id}'''.format(id = points_id)]
## % of residential dwellings < 400 m of a public transport stop with a scheduled service at least every 30 minutes between 7am and 7pm on a normal weekday (= a combined measure of proximity and frequency)
trans_6_state_locale_2016 = ['''
any_pt_30.ind_hard AS trans_6_{state}_{locale}_2016_hard,
any_pt_30.ind_soft AS trans_6_{state}_{locale}_2016_soft,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN (SELECT * FROM od_closest WHERE dest = 7) any_pt_30 ON p.{id} = any_pt_30.{id}'''.format(id = points_id)]
## < / >= 95% of residential dwellings < 400 m of public open space
## NOTE : This is identical to national indicator
pos_1_vic_melb_2016 = ['''
pos_400.ind_hard AS pos_1_vic_melb_2016_hard,
pos_400.ind_soft AS pos_1_vic_melb_2016_soft,''',
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(ind_hard,0) AS ind_hard, COALESCE(ind_soft,0) AS ind_soft FROM parcel_dwellings LEFT JOIN od_pos ON parcel_dwellings.{id} = od_pos.{id} AND threshold = 400 AND query = '') pos_400 ON p.{id} = pos_400.{id}'''.format(id = points_id)]
## < / >= 100% of residential cadastre < 300 m of any public open space
pos_2_wa_perth_2016 = ['''
pos_300.ind_hard AS pos_2_wa_perth_2016_hard,
pos_300.ind_soft AS pos_2_wa_perth_2016_soft,''',
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(ind_hard,0) AS ind_hard, COALESCE(ind_soft,0) AS ind_soft FROM parcel_dwellings LEFT JOIN od_pos ON parcel_dwellings.{id} = od_pos.{id} AND threshold = 300 AND query = '') pos_300 ON p.{id} = pos_300.{id}'''.format(id = points_id)]

## < / >= 50% of residential dwellings < 400 m of any local park >0.4 to <=1 ha
pos_3_wa_perth_2016 = ['''
pos_3_wa.ind_hard AS pos_3_wa_perth_2016_hard,
pos_3_wa.ind_soft AS pos_3_wa_perth_2016_soft,''',
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(ind_hard,0) AS ind_hard, COALESCE(ind_soft,0) AS ind_soft FROM parcel_dwellings LEFT JOIN od_pos ON parcel_dwellings.{id} = od_pos.{id} AND threshold = 400 AND query = 'area_ha > 0.4 AND area_ha <= 1 ') pos_3_wa ON p.{id} = pos_3_wa.{id}'''.format(id = points_id)]

## < / >= 50% of residential dwellings < 800 m of any neighbourhood park >1 ha - <= 5ha
pos_4_wa_perth_2016 = ['''
pos_4_wa.ind_hard AS pos_4_wa_perth_2016_hard,
pos_4_wa.ind_soft AS pos_4_wa_perth_2016_soft,''',
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(ind_hard,0) AS ind_hard, COALESCE(ind_soft,0) AS ind_soft FROM parcel_dwellings LEFT JOIN od_pos ON parcel_dwellings.{id} = od_pos.{id} AND threshold = 800 AND query = 'area_ha > 1 AND area_ha <= 5') pos_4_wa ON p.{id} = pos_4_wa.{id}'''.format(id = points_id)]

## < / >= 50% of residential dwellings < 2 km of any district park >5 ha (<=20 ha)
pos_5_wa_perth_2016 = ['''
pos_5_wa.ind_hard AS pos_5_wa_perth_2016_hard,
pos_5_wa.ind_soft AS pos_5_wa_perth_2016_soft,''',
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(ind_hard,0) AS ind_hard, COALESCE(ind_soft,0) AS ind_soft FROM parcel_dwellings LEFT JOIN od_pos ON parcel_dwellings.{id} = od_pos.{id} AND threshold = 2000 AND query = 'area_ha > 5 AND area_ha <= 20') pos_5_wa ON p.{id} = pos_5_wa.{id}'''.format(id = points_id)]

## </> 90% of residential dwellings < 400 m of a neighbourhood recreation park >0.5 ha
pos_6_qld_bris_2016 = ['''
pos_6_qld.ind_hard AS pos_6_qld_bris_2016_hard,
pos_6_qld.ind_soft AS pos_6_qld_bris_2016_soft,''',
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(ind_hard,0) AS ind_hard, COALESCE(ind_soft,0) AS ind_soft FROM parcel_dwellings LEFT JOIN od_pos ON parcel_dwellings.{id} = od_pos.{id} AND threshold = 400 AND query = 'area_ha > 0.5') pos_6_qld ON p.{id} = pos_6_qld.{id}'''.format(id = points_id)]
## </> 90% of residential dwellings < 2.5 km of a district recreation park >5 ha
pos_7_qld_bris_2016 = ['''
pos_7_qld.ind_hard AS pos_7_qld_bris_2016_hard,
pos_7_qld.ind_soft AS pos_7_qld_bris_2016_soft,''',
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(ind_hard,0) AS ind_hard, COALESCE(ind_soft,0) AS ind_soft FROM parcel_dwellings LEFT JOIN od_pos ON parcel_dwellings.{id} = od_pos.{id} AND threshold = 2500 AND query = 'area_ha > 5') pos_7_qld ON p.{id} = pos_7_qld.{id}'''.format(id = points_id)]
## < / >= 50% of residential dwellings < 400 m of a park >0.5 ha
pos_8_nsw_syd_2016 = ['''
pos_8_nsw.ind_hard AS pos_8_nsw_syd_2016_hard,
pos_8_nsw.ind_soft AS pos_8_nsw_syd_2016_soft,''',
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(ind_hard,0) AS ind_hard, COALESCE(ind_soft,0) AS ind_soft FROM parcel_dwellings LEFT JOIN od_pos ON parcel_dwellings.{id} = od_pos.{id} AND threshold = 400 AND query = 'area_ha > 0.5') pos_8_nsw ON p.{id} = pos_8_nsw.{id}'''.format(id = points_id)]
## < / >= 50% of residential dwellings < 2 km of a park >2 ha
pos_9_nsw_syd_2016 = ['''
pos_9_nsw.ind_hard AS pos_9_nsw_syd_2016_hard,
pos_9_nsw.ind_soft AS pos_9_nsw_syd_2016_soft,''',
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(ind_hard,0) AS ind_hard, COALESCE(ind_soft,0) AS ind_soft FROM parcel_dwellings LEFT JOIN od_pos ON parcel_dwellings.{id} = od_pos.{id} AND threshold = 2000 AND query = 'area_ha > 2') pos_9_nsw ON p.{id} = pos_9_nsw.{id}'''.format(id = points_id)]
## % residential dwellings < 400 m of POS
pos_10_state_locale_2016 = ['''
pos_400.ind_hard AS pos_10_{state}_{locale}_2016_hard,
pos_400.ind_soft AS pos_10_{state}_{locale}_2016_soft,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(ind_hard,0) AS ind_hard, COALESCE(ind_soft,0) AS ind_soft FROM parcel_dwellings LEFT JOIN od_pos ON parcel_dwellings.{id} = od_pos.{id} AND threshold = 400 AND query = '') pos_400 ON p.{id} = pos_400.{id}'''.format(id = points_id)]
## % residential dwellings < 400 m of POS > 1.5 ha
pos_11_state_locale_2016 = ['''
pos_400_large.ind_hard AS pos_11_{state}_{locale}_2016_hard,
pos_400_large.ind_soft AS pos_11_{state}_{locale}_2016_soft,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(ind_hard,0) AS ind_hard, COALESCE(ind_soft,0) AS ind_soft FROM parcel_dwellings LEFT JOIN od_pos ON parcel_dwellings.{id} = od_pos.{id} AND threshold = 400 AND query = 'area_ha > 1.5') pos_400_large ON p.{id} = pos_400_large.{id}'''.format(id = points_id)]
## KNIME: % of suburb area that is parkland
pos_12_state_locale_2016 = ["",""]

## KNIME: % of residential dwellings meeting 30/40 measure of unaffordable housing
hous_1_state_locale_2016 = ["",""]

## KNIME: % of residential dwellings renting as a proportion of total
hous_2_state_locale_2016 = ["",""]

## KNIME: % of employed persons working in the LGA in which their suburb resides
hous_1_state_locale_2016 = ["",""]

## KNIME: % of employed persons working in the region (i.e. SA4) in which their suburb resides
hous_2_state_locale_2016 = ["",""]

## KNIME: % of employed persons aged 15 and over using active transport to travel to work
hous_3_state_locale_2016 = ["",""]

## KNIME: % of employed persons aged 15 and over using public transport to travel to work
hous_4_state_locale_2016 = ["",""]

## KNIME: % of employed persons aged 15 and over using private vehicle/s to travel to work
hous_5_state_locale_2016 = ["",""]

## [NOT average ratio of green grocers and/or supermarkets to fast food outlets < 1600m?? as per basecamp discussion with JR  13 July 2018] 
## average proportion of supermarkets to supermarkets and fast food outlets combined < 3200 m 
food_1_state_locale_2016 = ['''
ind_foodratio.supermarket_proportion AS food_1_{state}_{locale}_2016,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN ind_foodratio ON p.{id} = ind_foodratio.{id}'''.format(id = points_id)]
## % residential dwellings < 1km of a supermarket
food_2_state_locale_2016 = ['''
ind_supermarket1000.ind_sm1000_hard AS food_2_{state}_{locale}_2016_hard,
ind_supermarket1000.ind_sm1000_soft AS food_2_{state}_{locale}_2016_soft,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN ind_supermarket1000 ON p.{id} = ind_supermarket1000.{id}'''.format(id = points_id)]
## average number of on-licenses < 400 m
alc_1_state_locale_2016 = ['''
alc_on.count_coalesced AS alc_1_{state}_{locale}_2016,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(od_counts.count,0) AS count_coalesced FROM parcel_dwellings LEFT JOIN od_counts ON parcel_dwellings.{id} = od_counts.{id} AND dest = 1) alc_on ON p.{id} = alc_on.{id}'''.format(id = points_id)]

## average number of off-licenses < 800 m
alc_2_state_locale_2016 = ['''
alc_off.count_coalesced AS alc_2_{state}_{locale}_2016,'''.format(state = state.lower(), locale = locale.lower()),
'''LEFT JOIN (SELECT parcel_dwellings.{id}, COALESCE(od_counts.count,0) AS count_coalesced FROM parcel_dwellings LEFT JOIN od_counts ON parcel_dwellings.{id} = od_counts.{id} AND dest = 0) alc_off ON p.{id} = alc_off.{id}'''.format(id = points_id)]

ind_queries = ''
ind_sources = ''
ind_source_unique = []
null_query_summary = ''
null_query_combined = ''
ind_names = []
print("Preparing relevant indicator queries for locale of {}:".format(locale))
for ind in ind_list:
  new_ind = globals()[ind]
  if new_ind[0] != "":
    # Build SQL query for specific indicator attributes relevant to locale
    ind_queries = '{previous_ind_queries}{new_ind_query}'.format(previous_ind_queries = ind_queries,
                                                                 new_ind_query        = new_ind[0])
    # To aid post-table creation diagnostics, prepare a list of not null queries 
    comma = ','
    plus = '+'
    if len(null_query_summary) == 0:
      comma = ''
      plus  = '+'
    nested_ind_list = new_ind[0].split(',')[0:new_ind[0].count(',')]
    for nind in nested_ind_list:
      ind_name = nind.split()[-1]
      ind_names.append(ind_name)
      null_query_summary = '{prev} {comma} SUM(({ind} IS NULL::int)) AS {ind} '.format(prev = null_query_summary,
                                                                                          comma = comma,
                                                                                          ind   = ind_name)
      null_query_combined = '{prev} {plus} ({ind} IS NULL::int)'.format(prev = null_query_combined,
                                                                            plus = plus,
                                                                            ind   = ind_name)                                                       
    # Build source query ensuring sources are only listed once 
    # (they may be re-used by indicators, but should only appear once in SQL query)    
    if new_ind[1] not in ind_source_unique:
      ind_source_unique.append(new_ind[1])
      ind_sources = '{previous_ind_source} \n{new_ind_source}'.format(previous_ind_source = ind_sources,
                                                                      new_ind_source      = new_ind[1])
    print('  - {}'.format(ind))
print("Done.\n")

create_parcel_indicators = '''
DROP TABLE IF EXISTS parcel_indicators;
CREATE TABLE parcel_indicators AS
SELECT
p.{id}                   ,
p.mb_code_20             ,
p.count_objectid         ,
p.point_x                ,
p.point_y                ,
p.hex_id                 ,
abs.mb_code_2016         ,
abs.mb_category_name_2016,
abs.dwelling             ,
abs.person               ,
abs.sa1_maincode         ,
abs.sa2_name_2016        ,
abs.sa3_name_2016        ,
abs.sa4_name_2016        ,
abs.gccsa_name           ,
abs.state_name           ,
non_abs.ssc_code_2016    ,
non_abs.ssc_name_2016    ,
non_abs.lga_code_2016    ,
non_abs.lga_name_2016    ,
{indicators}             
p.geom                   
FROM
parcel_dwellings p                                                                                 
LEFT JOIN abs_linkage abs ON p.mb_code_20 = abs.mb_code_2016
LEFT JOIN non_abs_linkage non_abs ON p.{id} = non_abs.{id}
{sources}
'''.format(id = points_id, indicators = ind_queries, sources = ind_sources)


null_query_summary_table = '''
DROP TABLE IF EXISTS parcel_inds_null_summary; 
CREATE TABLE parcel_inds_null_summary AS
SELECT {null_query_summary} 
FROM parcel_indicators;
'''.format(null_query_summary = null_query_summary)

null_query_combined_table = '''
DROP TABLE IF EXISTS parcel_inds_null_tally; 
CREATE TABLE parcel_inds_null_tally AS
SELECT {id},
       {null_query_combined} AS null_tally,
       {total_inds} AS total_inds
FROM parcel_indicators;
'''.format(id = points_id,
           null_query_combined = null_query_combined,
           total_inds = len(ind_list))

          
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

print("Creating compiled set of parcel level indicators...")
print("SQL query:")
print(create_parcel_indicators)
curs.execute(create_parcel_indicators)
conn.commit()
print("Done.")

print("Creating summary of nulls per indicator... ")
print("SQL query:")
print(null_query_summary_table)
curs.execute(null_query_summary_table)
conn.commit()
print("Done.")

print("Creating row-wise tally of nulls for each parcel...")
print("SQL query:")
print(null_query_combined_table)
curs.execute(null_query_combined_table)
conn.commit()
print("Done.\n")

df = pandas.read_sql_query('SELECT * FROM "parcel_inds_null_summary";',con=engine)
df = df.transpose()
df.columns = ['Null count']
print("Summary of nulls by {} variables for {} of {} in state of {}:".format(len(ind_list),region,locale,state))
print(df)

df2 = pandas.read_sql_query('SELECT * FROM "parcel_inds_null_tally";',con=engine)
print("Summary of row-wise null values across {} variables:".format(len(ind_list)))
print(df2['null_tally'].describe().round(2))

df.to_sql(name='parcel_ind_null_summary_t',con=engine,if_exists='replace')
df2['null_tally'].describe().round(2).to_sql(name='parcel_inds_null_tally_summary',con=engine,if_exists='replace')
ind_matrix.to_sql(name='ind_description',con=engine,if_exists='replace')

print("\n Nulls by indicator and Section of state")
for ind in ind_names:
  print("\n{}".format(ind))
  null_ind = pandas.read_sql_query("SELECT sos_name_2016, COUNT(*) null_count FROM parcel_indicators p LEFT JOIN parcel_sos sos ON p.gnaf_pid = sos.gnaf_pid WHERE {ind} IS NULL GROUP BY sos_name_2016;".format(ind = ind),con=engine)
  if len(null_ind) != 0:
    print(null_ind)
  if len(null_ind) == 0:
    print("No null values")


print("\nPostgresql summary tables containing the above were created:")
print("To view a description of all indicators for your region: SELECT * FROM ind_description;")
print("To view a summary of by variable name: SELECT * FROM parcel_ind_null_summary_t;")
print("To view a summary of row-wise null values: SELECT * FROM parcel_inds_null_tally_summary;")
print("To view a summary of null values for a particular indicator stratified by section of state:")
print(" SELECT sos_name_2016, COUNT(*) indicator_null_count FROM parcel_indicators p LEFT JOIN parcel_sos sos ON p.{id} = sos.{id} WHERE indicator IS NULL GROUP BY sos_name_2016;")

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
