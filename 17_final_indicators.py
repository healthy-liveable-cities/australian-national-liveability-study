# Script:  17_final_indicators_first_ass.py
# Purpose: Create final indicators for national liveability project
#          First pass indicators --- parcel level, area level thresholds not applied
# Author:  Carl Higgs 
# Date:    20180717

import os
import sys
import time
import psycopg2 

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

# for web --- schema domain_idx_state_city_year
## Note ambiguity b/w parcels and dwellings --- I will aim to produce two outputs (maybe three) for each ind: residential parcels (this may be our approximation to cadastre?); residential dwellings (based on Meshblock dwelling counts; Maybe in addition, residents (based on MB persons)).


# KNIME 
  # walk_1_vic_melb_2016
  ## % of street blocks with a perimeter of < 720 m (i.e. between 120 m and 240 m long and 60 m and 120 m wide)

# walk_2_vic_melb_2016
## < / >= 80% dwellings < 1 km of an activity centre (with a supermarket?? written, but a necessary condition of activity centre, i think?)
walk_2_vic_melb_2016 = '''(CASE WHEN ind_activity.distance <  1000 THEN 1
      WHEN ind_activity.distance >= 1000 THEN 0
      ELSE NULL END) as walk_12_{region}_{locale}_2016_hard,
1-1/(1+exp(-5*(ind_activity.distance-1000)/1000::double precision)) AS walk_12_{region}_{locale}_2016_soft,
'''

# KNIME 
  # walk_3_vic_melb_2016
  ## </ >= 15 dwellings per net developable hectare
  
  # walk_4_wa_perth_2016
  ## % of street blocks with a perimeter of < 720 m (i.e. <240 m long and <120 m wide)

# NA for moment
  # walk_5_wa_perth_2016
  ## % dwellings < 400 m network distance of a secondary or district centre or < 200 m network distance of a neighbourhood centre


# KNIME 
  # walk_6_wa_perth_2016
  ## < / >= 26 dwellings per net developable hectare
  
  # walk_7_wa_perth_2016
  ## % residential lots by size: ≤350m2; >350 - ≤550m2; >550 - ≤750m2; >750 - ≤950m2; >950 m2
  
  # walk_8_qld_bris_2016
  ## % of street blocks with a perimeter of < 560 m (i.e. between 100 m and 200 m long and 40 m and 80 m wide)
  
  # walk_9_qld_bris_2016
  ## < / >= 15 dwellings per hectare for suburban areas
  
  # walk_10_qld_bris_2016
  ## < / >= 30 dwellings per hectare for urban areas ( higher density neighbourhoods typically located around a major centre or secondary centre or large transit node)
  
  # walk_11_nsw_syd_2016
  ## < / >= 15 dwellings per hectare for new residential release areas

# walk_12_state_locale_2016
## Average distance to closest activity centre, where activity centre is defined as a supermarket < a commercial zoned Mesh Block
walk_12_state_locale_2016 = '''ind_activity.distance AS walk_12_{region}_{locale}_2016,
'''

## Bec workflow
  # walk_13_state_locale_2016
  ## average pedshed ratio, defined as the area of properties adjoining the 400 m pedestrian network distance from each residential dwelling divided by the radial "crow flies" area < 400 m; i.e. 50.2 ha.
  
# walk_14_state_locale_2016
## average number of daily living types present, measured as a score of 0-3, with 1 point for each category of (convenience store/petrol station/newsagent, PT stop, supermarket) < 1600m network distance
walk_14_state_locale_2016 = '''ind_daily_living.dl_hard AS walk_14_{region}_{locale}_2016_hard,
ind_daily_living.dl_soft AS walk_14_{region}_{locale}_2016_soft,
'''

# walk_15_state_locale_2016
## street connectivity, measured as 3+ leg intersection count/area in km2, where the area is computed from the 1600m pedestrian-accessible street network buffered by 50m
walk_15_state_locale_2016 = '''sc_nh1600m.sc_nh1600m AS walk_15_{region}_{locale}_2016,
'''

# walk_16_state_locale_2016
## dwelling density, calculated as the aggregate sum of dwellings recorded in the meshblock polygons intersecting the 1600m pedestrian-accessible street network once buffered by 50m ("neighbourhood") and divided by the "neighbourhood" area in hectares.
walk_16_state_locale_2016 = '''dd_nh1600m.dd_nh1600m AS walk_16_state_locale_2016,
'''

# walk_17_state_locale_2016
## composite walkability index, combining street connectivity, dwelling density and daily living destinations
walk_17_state_locale_2016 = '''ind_walkability.wa_hard AS walk_17_{region}_{locale}_2016_hard,
ind_walkability.wa_soft AS walk_17_{region}_{locale}_2016_soft,
'''

# trans_1_vic_melb_2016
## < / >= 95% of of residential cadastre with access to bus stop < 400 m OR < 600m of a tram stop OR < 800 m of a train station
trans_1_vic_melb_2016 = '''(CASE WHEN SUM(COALESCE(vic_pt.ind_hard,0)) > 0 THEN 1 ELSE 0 END) AS trans_1_vic_melb_2016_hard,
MAX(COALESCE(vic_pt.ind_soft,0)) AS trans_1_vic_melb_2016_soft,
'''


# trans_2_vic_melb_2016
## < / >= 95% of dwellings with access to bus stop < 400 m
trans_2_vic_melb_2016 = '''vic_bus.ind_hard AS trans_2_vic_melb_2016_hard,
vic_bus.ind_soft AS trans_2_vic_melb_2016_soft,
'''


# NA for moment
  # trans_3_wa_perth_2016
  ## < / >= 60% of residential cadastre < 400m walk from a neighbourhood or town centre, or a bus stop, or in a 800m walk from a railway station.

# trans_4_qld_bris_2016
## < / >= 100% of residential cadastre < 400 metres of an existing or planned public transport stop
trans_4_qld_bris_2016 = '''(CASE WHEN qld_pt.distance <  400  THEN 1
      WHEN qld_pt.distance >= 400 THEN 0
      ELSE NULL END) as trans_4_qld_bris_2016_hard,
 1-1/(1+exp(-5*(qld_pt.distance-400)/400::double precision)) AS trans_4_qld_bris_2016_soft
 '''

# trans_5_nsw_syd_2016
## < / >= 100% of residential cadastre < 400 m of a bus stop every 30 min OR < 800 m of a train station every 15 min
trans_5_nsw_syd_2016 = '''(CASE WHEN SUM(COALESCE(nsw_pt.ind_hard,0)) > 0 THEN 1 ELSE 0 END) AS trans_5_nsw_syd_2016_hard,
MAX(COALESCE(nsw_pt.ind_soft,0)) AS trans_5_nsw_syd_2016_soft,
'''

# trans_6_state_locale_2016
## % of residential dwellings < 400 m of a public transport stop with a scheduled service at least every 30 minutes between 7am and 7pm on a normal weekday (= a combined measure of proximity and frequency)
trans_6_state_locale_2016 = '''any_pt_30.ind_hard AS trans_6_{region}_{locale}_2016_hard,
any_pt_30.ind_soft AS trans_6_{region}_{locale}_2016_soft,
'''

# pos_1_vic_melb_2016
## < / >= 95% of residential dwellings < 400 m of public open space
## NOTE : This is identical to national indicator
pos_400.ind_hard AS pos_1_vic_melb_2016_hard,
pos_400.ind_soft AS pos_1_vic_melb_2016_soft,

# (SELECT * FROM od_pos WHERE threshold = 400) pos_any

# pos_2_wa_perth_2016
## < / >= 100% of residential cadastre < 300 m of any public open space
pos_300.ind_hard AS pos_2_wa_perth_2016_hard,
pos_300.ind_soft AS pos_2_wa_perth_2016_soft,


# pos_3_wa_perth_2016
## < / >= 50% of residential dwellings < 400 m of any local park >0.4 to <=1 ha
pos_3_wa.ind_hard AS pos_3_wa_perth_2016_hard,
pos_3_wa.ind_soft AS pos_3_wa_perth_2016_soft,


# pos_4_wa_perth_2016
## < / >= 50% of residential dwellings < 800 m of any neighbourhood park >1 ha - <= 5ha
pos_4_wa.ind_hard AS pos_4_wa_perth_2016_hard,
pos_4_wa.ind_soft AS pos_4_wa_perth_2016_soft,


# pos_5_wa_perth_2016
## < / >= 50% of residential dwellings < 2 km of any district park >5 ha (<=20 ha)
pos_5_wa.ind_hard AS pos_5_wa_perth_2016_hard,
pos_5_wa.ind_soft AS pos_5_wa_perth_2016_soft,

# pos_6_qld_bris_2016
## </> 90% of residential dwellings < 400 m of a neighbourhood recreation park >0.5 ha
pos_6_qld.ind_hard AS pos_6_qld_bris_2016_hard,
pos_6_qld.ind_soft AS pos_6_qld_bris_2016_soft,

# pos_7_qld_bris_2016
## </> 90% of residential dwellings < 2.5 km of a district recreation park >5 ha
pos_7_qld.ind_hard AS pos_7_qld_bris_2016_hard,
pos_7_qld.ind_soft AS pos_7_qld_bris_2016_soft,

# pos_8_nsw_syd_2016
## < / >= 50% of residential dwellings < 400 m of a park >0.5 ha
pos_8_nsw.ind_hard AS pos_8_nsw_syd_2016_hard,
pos_8_nsw.ind_soft AS pos_8_nsw_syd_2016_soft,

# pos_9_nsw_syd_2016
## < / >= 50% of residential dwellings < 2 km of a park >2 ha
pos_9_nsw.ind_hard AS pos_9_nsw_syd_2016_hard,
pos_9_nsw.ind_soft AS pos_9_nsw_syd_2016_soft,

# pos_10_state_locale_2016
## % residential dwellings < 400 m of POS
pos_400.ind_hard AS pos_10_{region}_{locale}_2016_hard,
pos_400.ind_soft AS pos_10_{region}_{locale}_2016_soft,

# pos_11_state_locale_2016
## % residential dwellings < 400 m of POS > 1.5 ha
pos_400_large.ind_hard AS pos_11_{region}_{locale}_2016_hard,
pos_400_large.ind_soft AS pos_11_{region}_{locale}_2016_soft,


# KNIME 
  # pos_12_state_locale_2016
  ## % of suburb area that is parkland
  
  # hous_1_state_locale_2016
  ## % of residential dwellings meeting 30/40 measure of unaffordable housing
  
  # hous_2_state_locale_2016
  ## % of residential dwellings renting as a proportion of total
  
  # hous_1_state_locale_2016
  ## % of employed persons working in the LGA in which their suburb resides
  
  # hous_2_state_locale_2016
  ## % of employed persons working in the region (i.e. SA4) in which their suburb resides
  
  # hous_3_state_locale_2016
  ## % of employed persons aged 15 and over using active transport to travel to work
  
  # hous_4_state_locale_2016
  ## % of employed persons aged 15 and over using public transport to travel to work
  
  # hous_5_state_locale_2016
  ## % of employed persons aged 15 and over using private vehicle/s to travel to work

# food_1_state_locale_2016
## [NOT average ratio of green grocers and/or supermarkets to fast food outlets < 1600m?? as per basecamp discussion with JR  13 July 2018] 
## average proportion of supermarkets to supermarkets and fast food outlets combined < 3200 m 
ind_foodratio.supermarket_proportion AS food_1_{region}_{locale}_2016,

# food_2_state_locale_2016
## % residential dwellings < 1km of a supermarket
ind_supermarket1000.ind_sm1000_hard AS food_2_{region}_{locale}_2016_hard,
ind_supermarket1000.ind_sm1000_soft AS food_2_{region}_{locale}_2016_soft,

# alc_1_state_locale_2016
## average number of on-licenses < 400 m
alc_on.count AS alc_1_{region}_{locale}_2016,


# alc_2_state_locale_2016
## average number of off-licenses < 800 m
alc_off.count AS alc_2_{region}_{locale}_2016





'''
CREATE TABLE parcel_indicators AS
SELECT
p.gnaf_pid               ,
p.mb_code_20             ,
p.count_objectid         ,
p.point_x                ,
p.point_y                ,
p.hex_id                 ,
p.geom                   ,
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

FROM
ind_activity
ind_daily_living
sc_nh1600m
dd_nh1600m
ind_walkability
(SELECT * FROM od_closest WHERE dest IN (9,10,11)) vic_pt
(SELECT * FROM od_closest WHERE dest = 9) vic_bus
(SELECT * FROM od_closest WHERE dest = 8) qld_pt
(SELECT * FROM od_closest WHERE dest IN (14,15) nsw_pt
(SELECT * FROM od_closest WHERE dest = 7) any_pt_30
(SELECT * FROM od_pos WHERE threshold = 300) pos_300
(SELECT * FROM od_pos WHERE threshold = 400) pos_400
(SELECT * FROM od_pos WHERE threshold = 400 AND query = 'area_ha > 1.5') pos_400_large
(SELECT * FROM od_pos WHERE threshold = 400 AND query = 'area_ha > 0.4 AND area_ha <= 1') pos_3_wa
(SELECT * FROM od_pos WHERE threshold = 400 AND query = 'area_ha > 1 AND area_ha <= 5') pos_4_wa
(SELECT * FROM od_pos WHERE threshold = 400 AND query = 'area_ha > 5 AND area_ha <= 20') pos_5_wa
(SELECT * FROM od_pos WHERE threshold = 400 AND query = 'area_ha > 0.5') pos_6_qld
(SELECT * FROM od_pos WHERE threshold = 2500 AND query = 'area_ha > 5') pos_7_qld
(SELECT * FROM od_pos WHERE threshold = 400 AND query = 'area_ha > 0.5') pos_8_nsw
(SELECT * FROM od_pos WHERE threshold = 2000 AND query = 'area_ha > 2') pos_9_nsw
ind_foodratio
ind_supermarket1000
(SELECT * FROM od_counts WHERE dest = 1) alc_on
(SELECT * FROM od_counts WHERE dest = 0) alc_off
'''.format(points_id,locale.lower(),region.lower())





# conn.close()

# # output to completion log    
# script_running_log(script, task, start, locale)
