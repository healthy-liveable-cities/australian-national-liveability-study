# Script:  17_final_indicators.py
# Purpose: Create final indicators for national liveability project
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

# walk_1_vic_melb_2016
## % of street blocks with a perimeter of < 720 m (i.e. between 120 m and 240 m long and 60 m and 120 m wide)

# walk_2_vic_melb_2016
## < / >= 80% dwellings < 1 km of an activity centre (with a supermarket?? written, but a necessary condition of activity centre, i think?)
ind_activity WHERE distance < 1000

# walk_3_vic_melb_2016
## </ >= 15 dwellings per net developable hectare


# walk_4_wa_perth_2016
## % of street blocks with a perimeter of < 720 m (i.e. <240 m long and <120 m wide)

# walk_5_wa_perth_2016
## % dwellings < 400 m network distance of a secondary or district centre or < 200 m network distance of a neighbourhood centre

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

# walk_13_state_locale_2016
## average pedshed ratio, defined as the area of properties adjoining the 400 m pedestrian network distance from each residential dwelling divided by the radial "crow flies" area < 400 m; i.e. 50.2 ha.

# walk_14_state_locale_2016
## average number of daily living types present, measured as a score of 0-3, with 1 point for each category of (convenience store/petrol station/newsagent, PT stop, supermarket) < 1600m network distance

# walk_15_state_locale_2016
## street connectivity, measured as 3+ leg intersection count/area in km2, where the area is computed from the 1600m pedestrian-accessible street network buffered by 50m

# walk_16_state_locale_2016
## dwelling density, calculated as the aggregate sum of dwellings recorded in the meshblock polygons intersecting the 1600m pedestrian-accessible street network once buffered by 50m ("neighbourhood") and divided by the "neighbourhood" area in hectares.

# walk_17_state_locale_2016
## composite walkability index, combining street connectivity, dwelling density and daily living destinations

# trans_1_vic_melb_2016
## < / >= 95% of of residential cadastre with access to bus stop < 400 m OR < 600m of a tram stop OR < 800 m of a train station

# trans_2_vic_melb_2016
## < / >= 95% of dwellings with access to bus stop < 400 m

# trans_3_wa_perth_2016
## < / >= 60% of residential cadastre < 400m walk from a neighbourhood or town centre, or a bus stop, or in a 800m walk from a railway station.

# trans_4_qld_bris_2016
## < / >= 100% of residential cadastre < 400 metres of an existing or planned public transport stop

# trans_5_nsw_syd_2016
## < / >= 100% of residential cadastre < 400 m of a bus stop every 30 min OR < 800 m of a train station every 15 min

# trans_6_state_locale_2016
## % of residential dwellings < < 400 m of a public transport stop with a scheduled service at least every 30 minutes between 7am and 7pm on a normal weekday (= a combined measure of proximity and frequency)

# pos_1_vic_melb_2016
## < / >= 95% of residential dwellings < 400 m of public open space

# pos_2_wa_perth_2016
## < / >= 100% of residential cadastre < 300 m of any public open space

# pos_3_wa_perth_2016
## < / >= 50% of residential dwellings < 400 m of any local park >0.4 to <=1 ha

# pos_4_wa_perth_2016
## < / >= 50% of residential dwellings < 800 m of any neighbourhood park >1 ha - <= 5ha

# pos_5_wa_perth_2016
## < / >= 50% of residential dwellings < 2 km of any district park >5 ha (<=20 ha)

# pos_6_qld_bris_2016
## </> 90% of residential dwellings < 400 m of a neighbourhood recreation park >0.5 ha

# pos_7_qld_bris_2016
## </> 90% of residential dwellings < 2.5 km of a district recreation park >5 ha

# pos_8_nsw_syd_2016
## < / >= 50% of residential dwellings < 400 m of a park >0.5 ha

# pos_9_nsw_syd_2016
## < / >= 50% of residential dwellings < 2 km of a park >2 ha

# pos_10_state_locale_2016
## % residential dwellings < 400 m of POS

# pos_11_state_locale_2016
## % residential dwellings < 400 m of POS > 1.5 ha

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
## average ratio of green grocers and/or supermarkets to fast food outlets < 1600m average proportion of supermarkets to supermarkets and fast food outlets combined < 3200 m (as per discussion with JR on 13 July 2018)

# food_2_state_locale_2016
## % residential dwellings < 1km of a supermarket

# alc_1_state_locale_2016
## average number of on-licenses < 400 m

# alc_2_state_locale_2016
## average number of off-licenses < 800 m


# create_liveability_ind_list = {'daily_living': dl,
#                                'walkability': wa,
#                                'activity_centres': activity,
#                                'supermarkets_1000': supermarkets_1000,
#                                'foodratio': foodratio}
        
# conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
# curs = conn.cursor()

# for ind in create_neighbourhood_ind_list.keys():
  # print("Creating {} indicator table... ".format(ind)),
  # curs.execute(create_neighbourhood_ind_list[ind])
  # conn.commit()
  # print("Done.")

# conn.close()

# # output to completion log    
# script_running_log(script, task, start, locale)
