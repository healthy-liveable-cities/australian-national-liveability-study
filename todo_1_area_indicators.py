# Script:  19_area_indicators.py
# Purpose: Create area level indicator tables
# Author:  Carl Higgs 
# Date:    20 July 2018


#### Sketch!! 

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
task = 'Create area level indicator tables for {}'.format(locale)


# Read in indicator description matrix
ind_matrix = pandas.read_csv(os.path.join(sys.path[0],'ind_study_region_matrix.csv'))
# Restrict to indicators associated with study region
ind_matrix = ind_matrix[ind_matrix['locale'].str.contains(locale)]
# Restrict to indicators with a defined source
ind_matrix = ind_matrix[pandas.notnull(ind_matrix['Source'])]


# Read in indicator description matrix
ind_matrix = pandas.read_csv(os.path.join(sys.path[0],'ind_study_region_matrix.csv'))

# Restrict to indicators associated with study region
ind_matrix = ind_matrix[ind_matrix['locale'].str.contains(locale)]

# Restrict to indicators with a defined source
ind_matrix = ind_matrix[pandas.notnull(ind_matrix['Source'])]

# Make concatenated indicator and tag name (e.g. 'walk_14' + 'hard')
# Tags could be useful later as can allow to search by name for e.g. threshold type,
# or other keywords (policy, binary, obsolete, planned --- i don't know, whatever)
# These tags are tacked on the end of the ind name seperated with underscores
ind_matrix['indicators'] = ind_matrix['ind'] + ind_matrix['tags'].fillna('')

# Compile list of indicators
ind_list = ind_matrix['indicators'].tolist()

# Note that postgresql ignores null values when calculating averages
# We can passively exploit this feature in the case of POS as those parcels with nulls will be 
# ignored --- this is exactly what we want.  Excellent.
ind_avg = ',\n'.join("AVG(" + ind_matrix['indicators'] + " ) AS " + ind_matrix['indicators'])

exclusion_criteria = 'WHERE  {0} NOT IN (SELECT DISTINCT({0}) FROM excluded_parcels)'.format(points_id.lower())
## I'm not sure if the below is still relevant
# parcelmb_exclusion_criteria = 'WHERE  parcelmb.{0} NOT IN (SELECT DISTINCT({0}) FROM excluded_parcels)'.format(points_id.lower())



# The shape file for map features are output 
map_features_outpath = os.path.join(folderPath,'study_region','wgs84_epsg4326','map_features')
locale_shp_outpath = os.path.join(map_features_outpath,db)

for dir in [map_features_outpath,locale_shp_outpath]:
  if not os.path.exists(dir):
      os.makedirs(dir)   

      
      
# SQL Settings
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

areas = {'mb_code_2016':'mb',
         'sa1_maincode':'sa1',
         'ssc_name':'ssc',
         'lga_name11':'lga'}
  
# create aggregated raw liveability estimates for selected area
for area_code in areas.keys:
  area = areas[area_code]
  print("Create aggregate indicator table {}... ".format(area_inds_{area})),
  createTable = '''
  DROP TABLE IF EXISTS area_inds_{area} ; 
  CREATE TABLE area_inds_{area} AS
  SELECT {area_code},
    {indicators}
    FROM parcel_indicators
    GROUP BY {area_code}
    ORDER BY {area_code} ASC;
  ALTER TABLE area_inds_{area} ADD PRIMARY KEY ({area_code});
  '''.format(area = area,area_code = area_code,indicators = ind_avg)
  curs.execute(createTable)
  conn.commit()
  print("Done.")
  
  
  #### Note - create a tag for units, like unit:percent  to identify those queries which need to be *100
  #### ... Or we could just do something manually like define a bespoke aggregation query where req'd... might be easier.
  ####  e.g. some are not just the indicator but a function of its threshold (e.g. (AVG(indicator)*100) > 50? 1:0)
  ### Yup - combo of these: in ind_matrix, create column "scale" (some are 1, others are 100), and column "split" which contains something like '>= 50' or '< 95' as reqd
  print("Create range smmary table {}... ".format(li_range_{area})),
  createTable = '''
  DROP TABLE IF EXISTS li_range_{area} ; 
  CREATE TABLE li_range_{area} AS
  SELECT {area_code},
    round(min(li_centile                  )::numeric,1)::text || ' - ' ||round(max(li_centile                  )::numeric,1)::text AS li_centile                 ,
    round(min(dd_nh1600m                  )::numeric,1)::text || ' - ' ||round(max(dd_nh1600m                  )::numeric,1)::text AS dd_nh1600m                  ,
    round(min(sc_nh1600m                  )::numeric,1)::text || ' - ' ||round(max(sc_nh1600m                  )::numeric,1)::text AS sc_nh1600m                  ,
    round(min(100*pos15000_access         )::numeric,1)::text || ' - ' ||round(max(100*pos15000_access         )::numeric,1)::text AS pos15000_access             ,
    round(min(100*sa1_prop_affordablehousing  )::numeric,1)::text || ' - ' ||round(max(100*sa1_prop_affordablehousing  )::numeric,1)::text AS sa1_prop_affordablehousing  ,
    round(min(100*sa2_prop_live_work_sa3  )::numeric,1)::text || ' - ' ||round(max(100*sa2_prop_live_work_sa3      )::numeric,1)::text AS sa2_prop_live_work_sa3,  
    round(min(100*communitycentre_1000m        )::numeric,1)::text || ' - ' ||round(max(100*communitycentre_1000m        )::numeric,1)::text AS communitycentre_1000m        ,  
    round(min(100*museumartgallery_3200m       )::numeric,1)::text || ' - ' ||round(max(100*museumartgallery_3200m       )::numeric,1)::text AS museumartgallery_3200m       ,  
    round(min(100*cinematheatre_3200m          )::numeric,1)::text || ' - ' ||round(max(100*cinematheatre_3200m          )::numeric,1)::text AS cinematheatre_3200m          ,  
    round(min(100*libraries_2014_1000m         )::numeric,1)::text || ' - ' ||round(max(100*libraries_2014_1000m         )::numeric,1)::text AS libraries_2014_1000m         ,  
    round(min(100*childcareoutofschool_1600m   )::numeric,1)::text || ' - ' ||round(max(100*childcareoutofschool_1600m   )::numeric,1)::text AS childcareoutofschool_1600m   ,  
    round(min(100*childcare_800m               )::numeric,1)::text || ' - ' ||round(max(100*childcare_800m               )::numeric,1)::text AS childcare_800m               ,  
    round(min(100*statesecondaryschools_1600m  )::numeric,1)::text || ' - ' ||round(max(100*statesecondaryschools_1600m  )::numeric,1)::text AS statesecondaryschools_1600m  ,  
    round(min(100*stateprimaryschools_1600m    )::numeric,1)::text || ' - ' ||round(max(100*stateprimaryschools_1600m    )::numeric,1)::text AS stateprimaryschools_1600m    ,  
    round(min(100*agedcare_2012_1000m          )::numeric,1)::text || ' - ' ||round(max(100*agedcare_2012_1000m          )::numeric,1)::text AS agedcare_2012_1000m          ,  
    round(min(100*communityhealthcentres_1000m )::numeric,1)::text || ' - ' ||round(max(100*communityhealthcentres_1000m )::numeric,1)::text AS communityhealthcentres_1000m ,  
    round(min(100*dentists_1000m               )::numeric,1)::text || ' - ' ||round(max(100*dentists_1000m               )::numeric,1)::text AS dentists_1000m               ,  
    round(min(100*gp_clinics_1000m             )::numeric,1)::text || ' - ' ||round(max(100*gp_clinics_1000m             )::numeric,1)::text AS gp_clinics_1000m             ,  
    round(min(100*maternalchildhealth_1000m    )::numeric,1)::text || ' - ' ||round(max(100*maternalchildhealth_1000m    )::numeric,1)::text AS maternalchildhealth_1000m    ,  
    round(min(100*swimmingpools_1200m          )::numeric,1)::text || ' - ' ||round(max(100*swimmingpools_1200m          )::numeric,1)::text AS swimmingpools_1200m          ,  
    round(min(100*sport_1200m                  )::numeric,1)::text || ' - ' ||round(max(100*sport_1200m                  )::numeric,1)::text AS sport_1200m                  ,  
    round(min(100*supermarkets_1000m           )::numeric,1)::text || ' - ' ||round(max(100*supermarkets_1000m           )::numeric,1)::text AS supermarkets_1000m           ,  
    round(min(100*conveniencestores_1000m      )::numeric,1)::text || ' - ' ||round(max(100*conveniencestores_1000m      )::numeric,1)::text AS conveniencestores_1000m      ,  
    round(min(100*petrolstations_1000m         )::numeric,1)::text || ' - ' ||round(max(100*petrolstations_1000m         )::numeric,1)::text AS petrolstations_1000m         ,  
    round(min(100*newsagents_1000m             )::numeric,1)::text || ' - ' ||round(max(100*newsagents_1000m             )::numeric,1)::text AS newsagents_1000m             ,  
    round(min(100*fishmeatpoultryshops_1600m   )::numeric,1)::text || ' - ' ||round(max(100*fishmeatpoultryshops_1600m   )::numeric,1)::text AS fishmeatpoultryshops_1600m   ,  
    round(min(100*fruitvegeshops_1600m         )::numeric,1)::text || ' - ' ||round(max(100*fruitvegeshops_1600m         )::numeric,1)::text AS fruitvegeshops_1600m         ,  
    round(min(100*pharmacy_1000m               )::numeric,1)::text || ' - ' ||round(max(100*pharmacy_1000m               )::numeric,1)::text AS pharmacy_1000m               ,  
    round(min(100*busstop2012_400m             )::numeric,1)::text || ' - ' ||round(max(100*busstop2012_400m             )::numeric,1)::text AS busstop2012_400m             ,  
    round(min(100*tramstops2012_600m           )::numeric,1)::text || ' - ' ||round(max(100*tramstops2012_600m           )::numeric,1)::text AS tramstops2012_600m           ,  
    round(min(100*trainstations2012_800m       )::numeric,1)::text || ' - ' ||round(max(100*trainstations2012_800m       )::numeric,1)::text AS trainstations2012_800m         
    FROM {2}.area_indicators_{1}  AS t1
    LEFT JOIN
    (SELECT detail_pid, 
            100*cume_dist() OVER(ORDER BY li_ci_est)::numeric AS li_centile
     FROM {2}.clean_li_parcel_ci_{1}) AS t2 ON t1.detail_pid = t2.detail_pid
    GROUP BY {0}
    ORDER BY {0} ASC;
  ALTER TABLE {2}.li_range_{1}_{0} ADD PRIMARY KEY ({0});
  '''.format(area,type,uli_schema)
  curs.execute(createTable)
  conn.commit()
  print("Created raw {1} range at {0} level for schema {2}".format(area,type,uli_schema))
    
# create aggregated raw liveability most for selected area
for type in ['hard','soft']:
  for area in ['mb_code_2016','sa1_maincode','sa2_name_2016','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.li_most_{1}_{0} ; 
    CREATE TABLE {2}.li_most_{1}_{0} AS
    SELECT {0},
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY li_centile                      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY li_centile                  )::numeric,1)::text AS li_centile                  ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY dd_nh1600m                      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY dd_nh1600m                  )::numeric,1)::text AS dd_nh1600m                  ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY sc_nh1600m                      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY sc_nh1600m                  )::numeric,1)::text AS sc_nh1600m                  ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*pos15000_access             )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*pos15000_access         )::numeric,1)::text AS pos15000_access             ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*sa1_prop_affordablehousing  )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*sa1_prop_affordablehousing  )::numeric,1)::text AS sa1_prop_affordablehousing,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*sa2_prop_live_work_sa3      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*sa2_prop_live_work_sa3  )::numeric,1)::text AS sa2_prop_live_work_sa3,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*communitycentre_1000m       )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*communitycentre_1000m        )::numeric,1)::text AS communitycentre_1000m        ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*museumartgallery_3200m      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*museumartgallery_3200m       )::numeric,1)::text AS museumartgallery_3200m       ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*cinematheatre_3200m         )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*cinematheatre_3200m          )::numeric,1)::text AS cinematheatre_3200m          ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*libraries_2014_1000m        )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*libraries_2014_1000m         )::numeric,1)::text AS libraries_2014_1000m         ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*childcareoutofschool_1600m  )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*childcareoutofschool_1600m   )::numeric,1)::text AS childcareoutofschool_1600m   ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*childcare_800m              )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*childcare_800m               )::numeric,1)::text AS childcare_800m               ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*statesecondaryschools_1600m )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*statesecondaryschools_1600m  )::numeric,1)::text AS statesecondaryschools_1600m  ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*stateprimaryschools_1600m   )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*stateprimaryschools_1600m    )::numeric,1)::text AS stateprimaryschools_1600m    ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*agedcare_2012_1000m         )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*agedcare_2012_1000m          )::numeric,1)::text AS agedcare_2012_1000m          ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*communityhealthcentres_1000m)::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*communityhealthcentres_1000m )::numeric,1)::text AS communityhealthcentres_1000m ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*dentists_1000m              )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*dentists_1000m               )::numeric,1)::text AS dentists_1000m               ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*gp_clinics_1000m            )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*gp_clinics_1000m             )::numeric,1)::text AS gp_clinics_1000m             ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*maternalchildhealth_1000m   )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*maternalchildhealth_1000m    )::numeric,1)::text AS maternalchildhealth_1000m    ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*swimmingpools_1200m         )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*swimmingpools_1200m          )::numeric,1)::text AS swimmingpools_1200m          ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*sport_1200m                 )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*sport_1200m                  )::numeric,1)::text AS sport_1200m                  ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*supermarkets_1000m          )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*supermarkets_1000m           )::numeric,1)::text AS supermarkets_1000m           ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*conveniencestores_1000m     )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*conveniencestores_1000m      )::numeric,1)::text AS conveniencestores_1000m      ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*petrolstations_1000m        )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*petrolstations_1000m         )::numeric,1)::text AS petrolstations_1000m         ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*newsagents_1000m            )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*newsagents_1000m             )::numeric,1)::text AS newsagents_1000m             ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*fishmeatpoultryshops_1600m  )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*fishmeatpoultryshops_1600m   )::numeric,1)::text AS fishmeatpoultryshops_1600m   ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*fruitvegeshops_1600m        )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*fruitvegeshops_1600m         )::numeric,1)::text AS fruitvegeshops_1600m         ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*pharmacy_1000m              )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*pharmacy_1000m               )::numeric,1)::text AS pharmacy_1000m               ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*busstop2012_400m            )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*busstop2012_400m             )::numeric,1)::text AS busstop2012_400m             ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*tramstops2012_600m          )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*tramstops2012_600m           )::numeric,1)::text AS tramstops2012_600m           ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*trainstations2012_800m      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*trainstations2012_800m       )::numeric,1)::text AS trainstations2012_800m         
      FROM {2}.area_indicators_{1} AS t1
      LEFT JOIN 
      (SELECT detail_pid, 
              100*cume_dist() OVER(ORDER BY li_ci_est)::numeric AS li_centile
       FROM {2}.clean_li_parcel_ci_{1}) AS t2 ON t1.detail_pid = t2.detail_pid
      GROUP BY {0}
      ORDER BY {0} ASC;
    ALTER TABLE {2}.li_most_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)
    curs.execute(createTable)
    conn.commit()
    print("Created raw {1} most at {0} level for schema {2}".format(area,type,uli_schema))
    
    
# create aggregated normalised liveability estimates for selected area
for type in ['hard','soft']:
  for area in ['mb_code_2016','sa1_maincode','sa2_name_2016','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.clean_li_mpi_norm_{1}_{0} ; 
    CREATE TABLE {2}.clean_li_mpi_norm_{1}_{0} AS
    SELECT {0},
      AVG(li_ci_est                   ) AS li_ci_est                   ,
      AVG(dd_nh1600m                  ) AS dd_nh1600m                  ,
      AVG(sc_nh1600m                  ) AS sc_nh1600m                  ,
      AVG(pos15000_access             ) AS pos15000_access             ,
      AVG(sa1_prop_affordablehousing  ) AS sa1_prop_affordablehousing  ,
      AVG(sa2_prop_live_work_sa3      ) AS sa2_prop_live_work_sa3      ,
      AVG(communitycentre_1000m        ) AS communitycentre_1000m        ,
      AVG(museumartgallery_3200m       ) AS museumartgallery_3200m       ,
      AVG(cinematheatre_3200m          ) AS cinematheatre_3200m          ,
      AVG(libraries_2014_1000m         ) AS libraries_2014_1000m         ,
      AVG(childcareoutofschool_1600m   ) AS childcareoutofschool_1600m   ,
      AVG(childcare_800m               ) AS childcare_800m               ,
      AVG(statesecondaryschools_1600m  ) AS statesecondaryschools_1600m  ,
      AVG(stateprimaryschools_1600m    ) AS stateprimaryschools_1600m    ,
      AVG(agedcare_2012_1000m          ) AS agedcare_2012_1000m          ,
      AVG(communityhealthcentres_1000m ) AS communityhealthcentres_1000m ,
      AVG(dentists_1000m               ) AS dentists_1000m               ,
      AVG(gp_clinics_1000m             ) AS gp_clinics_1000m             ,
      AVG(maternalchildhealth_1000m    ) AS maternalchildhealth_1000m    ,
      AVG(swimmingpools_1200m          ) AS swimmingpools_1200m          ,
      AVG(sport_1200m                  ) AS sport_1200m                  ,
      AVG(supermarkets_1000m           ) AS supermarkets_1000m           ,
      AVG(conveniencestores_1000m      ) AS conveniencestores_1000m      ,
      AVG(petrolstations_1000m         ) AS petrolstations_1000m         ,
      AVG(newsagents_1000m             ) AS newsagents_1000m             ,
      AVG(fishmeatpoultryshops_1600m   ) AS fishmeatpoultryshops_1600m   ,
      AVG(fruitvegeshops_1600m         ) AS fruitvegeshops_1600m         ,
      AVG(pharmacy_1000m               ) AS pharmacy_1000m               ,
      AVG(busstop2012_400m             ) AS busstop2012_400m             ,
      AVG(tramstops2012_600m           ) AS tramstops2012_600m           ,
      AVG(trainstations2012_800m       ) AS trainstations2012_800m       
      FROM  {2}.clean_li_parcel_ci_{1}
      GROUP BY {0}
      ORDER BY {0} ASC;
    ALTER TABLE {2}.clean_li_mpi_norm_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)    
    
    curs.execute(createTable)
    conn.commit()
    print("Created normalised {1} averages at {0} level for schema {2}".format(area,type,uli_schema))  

# create aggregated SD for normalised liveability estimates for selected area
for type in ['hard','soft']:
  for area in ['mb_code_2016','sa1_maincode','sa2_name_2016','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.clean_li_mpi_sd_{1}_{0} ; 
    CREATE TABLE {2}.clean_li_mpi_sd_{1}_{0} AS
    SELECT {0},
      stddev_pop(li_ci_est                   ) AS sd_li_ci_est                   ,
      stddev_pop(dd_nh1600m                  ) AS sd_dd_nh1600m                  ,
      stddev_pop(sc_nh1600m                  ) AS sd_sc_nh1600m                  ,
      stddev_pop(pos15000_access             ) AS sd_pos15000_access             ,
      stddev_pop(sa1_prop_affordablehousing  ) AS sd_sa1_prop_affordablehousing  ,
      stddev_pop(sa2_prop_live_work_sa3      ) AS sd_sa2_prop_live_work_sa3      ,
      stddev_pop(communitycentre_1000m       ) AS sd_communitycentre_1000m        ,
      stddev_pop(museumartgallery_3200m      ) AS sd_museumartgallery_3200m       ,
      stddev_pop(cinematheatre_3200m         ) AS sd_cinematheatre_3200m          ,
      stddev_pop(libraries_2014_1000m        ) AS sd_libraries_2014_1000m         ,
      stddev_pop(childcareoutofschool_1600m  ) AS sd_childcareoutofschool_1600m   ,
      stddev_pop(childcare_800m              ) AS sd_childcare_800m               ,
      stddev_pop(statesecondaryschools_1600m ) AS sd_statesecondaryschools_1600m  ,
      stddev_pop(stateprimaryschools_1600m   ) AS sd_stateprimaryschools_1600m    ,
      stddev_pop(agedcare_2012_1000m         ) AS sd_agedcare_2012_1000m          ,
      stddev_pop(communityhealthcentres_1000m) AS sd_communityhealthcentres_1000m ,
      stddev_pop(dentists_1000m              ) AS sd_dentists_1000m               ,
      stddev_pop(gp_clinics_1000m            ) AS sd_gp_clinics_1000m             ,
      stddev_pop(maternalchildhealth_1000m   ) AS sd_maternalchildhealth_1000m    ,
      stddev_pop(swimmingpools_1200m         ) AS sd_swimmingpools_1200m          ,
      stddev_pop(sport_1200m                 ) AS sd_sport_1200m                  ,
      stddev_pop(supermarkets_1000m          ) AS sd_supermarkets_1000m           ,
      stddev_pop(conveniencestores_1000m     ) AS sd_conveniencestores_1000m      ,
      stddev_pop(petrolstations_1000m        ) AS sd_petrolstations_1000m         ,
      stddev_pop(newsagents_1000m            ) AS sd_newsagents_1000m             ,
      stddev_pop(fishmeatpoultryshops_1600m  ) AS sd_fishmeatpoultryshops_1600m   ,
      stddev_pop(fruitvegeshops_1600m        ) AS sd_fruitvegeshops_1600m         ,
      stddev_pop(pharmacy_1000m              ) AS sd_pharmacy_1000m               ,
      stddev_pop(busstop2012_400m            ) AS sd_busstop2012_400m             ,
      stddev_pop(tramstops2012_600m          ) AS sd_tramstops2012_600m           ,
      stddev_pop(trainstations2012_800m      ) AS sd_trainstations2012_800m       
      FROM  {2}.clean_li_parcel_ci_{1}
      GROUP BY {0}
      ORDER BY {0} ASC;
    ALTER TABLE {2}.clean_li_mpi_sd_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)    
    
    curs.execute(createTable)
    conn.commit()
    print("Created SD for normalised {1} averages at {0} level for schema {2}".format(area,type,uli_schema))  
    
# create deciles of liveability estimates for selected area
for type in ['hard','soft']:
  for area in ['mb_code_2016','sa1_maincode','sa2_name_2016','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.clean_li_deciles_{1}_{0} ; 
    CREATE TABLE {2}.clean_li_deciles_{1}_{0} AS
    SELECT {0},
           round(10*cume_dist() OVER(ORDER BY li_ci_est                   )::numeric,0) as li_ci_est                   ,
           round(10*cume_dist() OVER(ORDER BY dd_nh1600m                  )::numeric,0) as dd_nh1600m                  ,
           round(10*cume_dist() OVER(ORDER BY sc_nh1600m                  )::numeric,0) as sc_nh1600m                  ,
           round(10*cume_dist() OVER(ORDER BY pos15000_access             )::numeric,0) as pos15000_access             ,
           round(10*cume_dist() OVER(ORDER BY sa1_prop_affordablehousing  )::numeric,0) as sa1_prop_affordablehousing  ,
           round(10*cume_dist() OVER(ORDER BY sa2_prop_live_work_sa3      )::numeric,0) as sa2_prop_live_work_sa3      ,
           round(10*cume_dist() OVER(ORDER BY communitycentre_1000m       )::numeric,0) as communitycentre_1000m       ,
           round(10*cume_dist() OVER(ORDER BY museumartgallery_3200m      )::numeric,0) as museumartgallery_3200m      ,
           round(10*cume_dist() OVER(ORDER BY cinematheatre_3200m         )::numeric,0) as cinematheatre_3200m         ,
           round(10*cume_dist() OVER(ORDER BY libraries_2014_1000m        )::numeric,0) as libraries_2014_1000m        ,
           round(10*cume_dist() OVER(ORDER BY childcareoutofschool_1600m  )::numeric,0) as childcareoutofschool_1600m  ,
           round(10*cume_dist() OVER(ORDER BY childcare_800m              )::numeric,0) as childcare_800m              ,
           round(10*cume_dist() OVER(ORDER BY statesecondaryschools_1600m )::numeric,0) as statesecondaryschools_1600m ,
           round(10*cume_dist() OVER(ORDER BY stateprimaryschools_1600m   )::numeric,0) as stateprimaryschools_1600m   ,
           round(10*cume_dist() OVER(ORDER BY agedcare_2012_1000m         )::numeric,0) as agedcare_2012_1000m         ,
           round(10*cume_dist() OVER(ORDER BY communityhealthcentres_1000m)::numeric,0) as communityhealthcentres_1000m,
           round(10*cume_dist() OVER(ORDER BY dentists_1000m              )::numeric,0) as dentists_1000m              ,
           round(10*cume_dist() OVER(ORDER BY gp_clinics_1000m            )::numeric,0) as gp_clinics_1000m            ,
           round(10*cume_dist() OVER(ORDER BY maternalchildhealth_1000m   )::numeric,0) as maternalchildhealth_1000m   ,
           round(10*cume_dist() OVER(ORDER BY swimmingpools_1200m         )::numeric,0) as swimmingpools_1200m         ,
           round(10*cume_dist() OVER(ORDER BY sport_1200m                 )::numeric,0) as sport_1200m                 ,
           round(10*cume_dist() OVER(ORDER BY supermarkets_1000m          )::numeric,0) as supermarkets_1000m          ,
           round(10*cume_dist() OVER(ORDER BY conveniencestores_1000m     )::numeric,0) as conveniencestores_1000m     ,
           round(10*cume_dist() OVER(ORDER BY petrolstations_1000m        )::numeric,0) as petrolstations_1000m        ,
           round(10*cume_dist() OVER(ORDER BY newsagents_1000m            )::numeric,0) as newsagents_1000m            ,
           round(10*cume_dist() OVER(ORDER BY fishmeatpoultryshops_1600m  )::numeric,0) as fishmeatpoultryshops_1600m  ,
           round(10*cume_dist() OVER(ORDER BY fruitvegeshops_1600m        )::numeric,0) as fruitvegeshops_1600m        ,
           round(10*cume_dist() OVER(ORDER BY pharmacy_1000m              )::numeric,0) as pharmacy_1000m              ,
           round(10*cume_dist() OVER(ORDER BY busstop2012_400m            )::numeric,0) as busstop2012_400m            ,
           round(10*cume_dist() OVER(ORDER BY tramstops2012_600m          )::numeric,0) as tramstops2012_600m          ,
           round(10*cume_dist() OVER(ORDER BY trainstations2012_800m      )::numeric,0) as trainstations2012_800m      
    FROM {2}.clean_li_mpi_norm_{1}_{0}
    ORDER BY {0} ASC;
    ALTER TABLE {2}.clean_li_deciles_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)

    curs.execute(createTable)
    conn.commit()
    print("Created {1} deciles at {0} level for schema {2}".format(area,type,uli_schema))  

# create percentiles of liveability estimates for selected area
for type in ['hard','soft']:
  for area in ['mb_code_2016','sa1_maincode','sa2_name_2016','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.clean_li_percentiles_{1}_{0} ; 
    CREATE TABLE {2}.clean_li_percentiles_{1}_{0} AS
    SELECT {0},
           round(100*cume_dist() OVER(ORDER BY li_ci_est                   )::numeric,0) as li_ci_est                   ,
           round(100*cume_dist() OVER(ORDER BY dd_nh1600m                  )::numeric,0) as dd_nh1600m                  ,
           round(100*cume_dist() OVER(ORDER BY sc_nh1600m                  )::numeric,0) as sc_nh1600m                  ,
           round(100*cume_dist() OVER(ORDER BY pos15000_access             )::numeric,0) as pos15000_access             ,
           round(100*cume_dist() OVER(ORDER BY sa1_prop_affordablehousing  )::numeric,0) as sa1_prop_affordablehousing  ,
           round(100*cume_dist() OVER(ORDER BY sa2_prop_live_work_sa3      )::numeric,0) as sa2_prop_live_work_sa3      ,
           round(100*cume_dist() OVER(ORDER BY communitycentre_1000m        )::numeric,0) as communitycentre_1000m        ,
           round(100*cume_dist() OVER(ORDER BY museumartgallery_3200m       )::numeric,0) as museumartgallery_3200m       ,
           round(100*cume_dist() OVER(ORDER BY cinematheatre_3200m          )::numeric,0) as cinematheatre_3200m          ,
           round(100*cume_dist() OVER(ORDER BY libraries_2014_1000m         )::numeric,0) as libraries_2014_1000m         ,
           round(100*cume_dist() OVER(ORDER BY childcareoutofschool_1600m   )::numeric,0) as childcareoutofschool_1600m   ,
           round(100*cume_dist() OVER(ORDER BY childcare_800m               )::numeric,0) as childcare_800m               ,
           round(100*cume_dist() OVER(ORDER BY statesecondaryschools_1600m  )::numeric,0) as statesecondaryschools_1600m  ,
           round(100*cume_dist() OVER(ORDER BY stateprimaryschools_1600m    )::numeric,0) as stateprimaryschools_1600m    ,
           round(100*cume_dist() OVER(ORDER BY agedcare_2012_1000m          )::numeric,0) as agedcare_2012_1000m          ,
           round(100*cume_dist() OVER(ORDER BY communityhealthcentres_1000m )::numeric,0) as communityhealthcentres_1000m ,
           round(100*cume_dist() OVER(ORDER BY dentists_1000m               )::numeric,0) as dentists_1000m               ,
           round(100*cume_dist() OVER(ORDER BY gp_clinics_1000m             )::numeric,0) as gp_clinics_1000m             ,
           round(100*cume_dist() OVER(ORDER BY maternalchildhealth_1000m    )::numeric,0) as maternalchildhealth_1000m    ,
           round(100*cume_dist() OVER(ORDER BY swimmingpools_1200m          )::numeric,0) as swimmingpools_1200m          ,
           round(100*cume_dist() OVER(ORDER BY sport_1200m                  )::numeric,0) as sport_1200m                  ,
           round(100*cume_dist() OVER(ORDER BY supermarkets_1000m           )::numeric,0) as supermarkets_1000m           ,
           round(100*cume_dist() OVER(ORDER BY conveniencestores_1000m      )::numeric,0) as conveniencestores_1000m      ,
           round(100*cume_dist() OVER(ORDER BY petrolstations_1000m         )::numeric,0) as petrolstations_1000m         ,
           round(100*cume_dist() OVER(ORDER BY newsagents_1000m             )::numeric,0) as newsagents_1000m             ,
           round(100*cume_dist() OVER(ORDER BY fishmeatpoultryshops_1600m   )::numeric,0) as fishmeatpoultryshops_1600m   ,
           round(100*cume_dist() OVER(ORDER BY fruitvegeshops_1600m         )::numeric,0) as fruitvegeshops_1600m         ,
           round(100*cume_dist() OVER(ORDER BY pharmacy_1000m               )::numeric,0) as pharmacy_1000m               ,
           round(100*cume_dist() OVER(ORDER BY busstop2012_400m             )::numeric,0) as busstop2012_400m             ,
           round(100*cume_dist() OVER(ORDER BY tramstops2012_600m           )::numeric,0) as tramstops2012_600m           ,
           round(100*cume_dist() OVER(ORDER BY trainstations2012_800m       )::numeric,0) as trainstations2012_800m       
    FROM {2}.clean_li_mpi_norm_{1}_{0} 
    ORDER BY {0} ASC;
    ALTER TABLE {2}.clean_li_percentiles_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)

    curs.execute(createTable)
    conn.commit()
    print("Created {1} percentiles at {0} level for schema {2}".format(area,type,uli_schema))  



# Create shape files for interactive map visualisation
areas = ['sa1_maincode','ssc_name','lga_name11']
short_area = ['sa1','ssc','lga']
area_strings = ['''t6.sa1_maincode AS sa1   ,
                   t6.suburb                ,
                   t6.lga                   ,
                   t6.resid_parcels         ,
                   t6.dwellings             ,
                   t6.resid_persons         ,
                   ''',
               ''' '-'::varchar          AS sa1   ,
                   t6.suburb       AS suburb,
                   t6.lga          AS lga   ,
                   t6.resid_parcels         ,
                   t6.dwellings             ,
                   t6.resid_persons         ,
                   ''',
               ''' '-'::varchar         AS sa1    ,
                   '-'::varchar         AS suburb ,
                   t6.lga_name11  AS lga    ,
                   t6.resid_parcels         ,
                   t6.dwellings             ,
                   t6.resid_persons         ,
                   ''']                        
geom_tables = ['''LEFT JOIN main_sa1_2016_aust_full AS t5 ON p.sa1_maincode = t5.sa1_maincode::numeric''',
               '''LEFT JOIN main_ssc_2016_aust AS t5      ON p.ssc_name = t5.ssc_name''',
               '''LEFT JOIN main_lga_2016_aust AS t5      ON p.lga_name11 = t5.lga_name11''']
area_tables = ['''LEFT JOIN sa1_area AS t6                ON p.sa1_maincode = t6.sa1_maincode''',
               '''LEFT JOIN ssc_area AS t6                ON p.ssc_name = t6.suburb''',
               '''LEFT JOIN lga_area AS t6                ON p.ssc_name = t6.suburb''']
community_code = ['''t5.sa1_7digit AS community_code,''',
                  '''CONCAT('SSC',t5.ssc_code_2::varchar) AS community_code,''',
                  '''CONCAT('LGA',t5.lga_code_2::varchar) AS community_code,''']

for area in areas:   
    createTable = '''DROP TABLE IF EXISTS clean_li_map_{area};
    CREATE TABLE clean_li_map_{area} AS
    SELECT {1}
    round(raw.walkability::numeric,1)  AS rh1,
    round(raw.daily_living::numeric,1) AS rh2,
    round(raw.dd_nh1600m::numeric,1)   AS rh3,
    round(raw.sc_nh1600m::numeric,1)   AS rh4,
    round(percwalkability::numeric,0)  AS ph1,
    round(percdaily_living::numeric,0) AS ph2,
    round(percdd_nh1600m::numeric,0)   AS ph3,
    round(percsc_nh1600m::numeric,0)   AS ph4,
    trh.walkability AS dh1,
    trh.daily_living AS dh2,
    trh.dd_nh1600m AS dh3,
    trh.sc_nh1600m AS dh4,
    tmh.walkability AS mh1,
    tmh.daily_living AS mh2,
    tmh.dd_nh1600m AS mh3,
    tmh.sc_nh1600m AS mh4,
    round({6}_lrs.walkability::numeric,1) AS rs1,
    round({6}_lrs.daily_living::numeric,1) AS rs2,
    round({6}_lps.walkability::numeric,0) AS ps1,
    round({6}_lps.daily_living::numeric,0) AS ps2,
    trs.walkability AS ds1,
    trs.daily_living AS ds2,
    tms.walkability AS ms1,
    tms.daily_living AS ms2,
    gid,        
    community_code,
    ST_TRANSFORM(geom,4326) AS geom              
    FROM {6}.li_raw_hard_{2}                    AS raw
    {3}
    {4}
    {5};'''.format(area = short_area[areas.index(area)],
                   area_strings[areas.index(area)],
                   area,
                   geom_tables[areas.index(area)],
                   area_tables[areas.index(area)],
                   community_code[areas.index(area)])
    print(createTable)
    curs.execute(createTable)
    conn.commit()
    command = 'pgsql2shp -f {0}{6}_clean_li_map_{1}.shp -h {2} -u {3} -P {4} {5} {6}.clean_li_map_{1}'.format(outpath,short_area[areas.index(area)],sqlDBHost,sqlUserName,sqlPWD,sqlDBName,uli_schema)
    sp.call(command.split())

print("--Created SA1, suburb and LGA level tables for map web app for schema {0}".format(uli_schema))      
conn.close()


conn.close()
  
# output to completion log    
script_running_log(script, task, start, locale)
