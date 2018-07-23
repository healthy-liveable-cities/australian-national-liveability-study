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


# load indicator linkage table
ind_matrix = pandas.read_csv(os.path.join(sys.path[0],'ind_study_region_matrix.csv'))

ind_matrix = ind_matrix[[locale,'Description']].dropna()
ind_list = ind_matrix[locale].tolist()


# ULI schema to which this script pertains 
# This could be custom (as in Liveability Index work); however we have set to public (the default)
uli_schema = 'public'

exclusion_criteria = 'WHERE  {0} NOT IN (SELECT DISTINCT({0}) FROM excluded_parcels)'.format(points_id.lower())
parcelmb_exclusion_criteria = 'WHERE  parcelmb.{0} NOT IN (SELECT DISTINCT({0}) FROM excluded_parcels)'.format(points_id.lower())



# The shape file for map features are output 
map_features_outpath = os.path.join(folderPath,'study_region','wgs84_epsg4326','map_features')
locale_shp_outpath = os.path.join(map_features_outpath,db)

for dir in [map_features_outpath,locale_shp_outpath]:
  if not os.path.exists(dir):
      os.makedirs(dir)   

      
      
# SQL Settings
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()



## Create schema for this set of liveability indicators;
## Note: if using 'public' this is not necessary, hence commented out.
## Only relevant if you have change uli_schema variable to custom string.
## createSchema = '''
#  # DROP SCHEMA IF EXISTS {0} CASCADE;
#  # CREATE SCHEMA {0};
#  # '''.format(uli_schema)
## curs.execute(createSchema)
## conn.commit()

for i in ['hard','soft']:  
  # create raw indicator table
  createTable = '''
  DROP TABLE IF EXISTS {3}.area_indicators_{1} ; 
  CREATE TABLE {3}.area_indicators_{1} AS    
  SELECT parcelmb.{0},
             abs_linkage.mb_code_2016,
             abs_linkage.sa1_maincode,
             abs_linkage.sa2_name_2016,
             abs_linkage.sa3_name_2016,
             abs_linkage.sa4_name_2016,
             abs_linkage.gccsa_name,
             abs_linkage.state_name,
             non_abs_linkage.ssc_name_2016,
             non_abs_linkage.lga_name_2016,
             li_ci_est,
             dd_nh1600m,
             sc_nh1600m,
             pos_greq15000m2_in_400m_{1} AS pos15000_access,
             sa1_prop_affordablehous_30_40 AS sa1_prop_affordablehousing,
             sa2_prop_live_work_sa3,
             communitycentre_1000m        ,
             museumartgallery_3200m       ,
             cinematheatre_3200m          ,
             libraries_2014_1000m         ,
             childcareoutofschool_1600m   ,
             childcare_800m               ,
             statesecondaryschools_1600m  ,
             stateprimaryschools_1600m    ,
             agedcare_2012_1000m          ,
             communityhealthcentres_1000m ,
             dentists_1000m               ,
             gp_clinics_1000m             ,
             maternalchildhealth_1000m    ,
             swimmingpools_1200m          ,
             sport_1200m                  ,
             supermarkets_1000m           ,
             conveniencestores_1000m      ,
             petrolstations_1000m         ,
             newsagents_1000m             ,
             fishmeatpoultryshops_1600m   ,
             fruitvegeshops_1600m         ,
             pharmacy_1000m               ,
             busstop2012_400m             ,
             tramstops2012_600m           ,
             trainstations2012_800m       
  FROM parcelmb 
  LEFT JOIN abs_linkage ON parcelmb.mb_code_2016 = abs_linkage.mb_code_2016
  LEFT JOIN non_abs_linkage ON parcelmb.{0} = non_abs_linkage.{0}
  LEFT JOIN {3}.clean_li_ci_{1}_est ON {3}.clean_li_ci_{1}_est.{0} = parcelmb.{0}
  LEFT JOIN ind_abs ON parcelmb.{0} = ind_abs.{0}
  LEFT JOIN dwelling_density ON parcelmb.{0} = dwelling_density.{0}
  LEFT JOIN street_connectivity ON parcelmb.{0} = street_connectivity.{0}
  LEFT JOIN ind_pos ON parcelmb.{0} = ind_pos.{0}
  LEFT JOIN ind_dest_{1} ON parcelmb.{0} = ind_dest_{1}.{0}
  {2};
  ALTER TABLE {3}.area_indicators_{1} ADD PRIMARY KEY ({0});
  '''.format(points_id.lower(),i,parcelmb_exclusion_criteria,uli_schema)

  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.area_indicators_{0}', with parcel level id, linkage codes, pLI estimates, and raw indicators".format(i,uli_schema))  

for type in ['hard','soft']:
  createTable = '''
  DROP TABLE IF EXISTS {1}.clean_li_percentile_{0};
  CREATE TABLE {1}.clean_li_percentile_{0} AS
  SELECT t1.detail_pid,
         round(100*cume_dist() OVER(ORDER BY li_ci_est)::numeric,0) as li_ci_est,
         geom
  FROM {1}.clean_li_parcel_ci_{0} AS t1
  LEFT JOIN parcel_xy AS t2 on t1.detail_pid = t2.detail_pid
  '''.format(type,uli_schema)

  curs.execute(createTable)
  conn.commit()
  print("Created {0} address-level percentiles for schema {1}".format(type,uli_schema))  
  
# create sa1 area linkage corresponding to later SA1 aggregate tables
createTable = '''  
  DROP TABLE IF EXISTS {0}.sa1_area;
  CREATE TABLE {0}.sa1_area AS
  SELECT sa1_maincode, 
  string_agg(distinct(ssc_name),',') AS suburb, 
  string_agg(distinct(lga_name11), ', ') AS lga
  FROM  {0}.raw_indicators_hard
  WHERE sa1_maincode IN (SELECT sa1_maincode FROM abs_2011_irsd)
  GROUP BY sa1_maincode
  ORDER BY sa1_maincode ASC;
  '''.format(uli_schema)
curs.execute(createTable)
conn.commit()

# create sa2 area linkage corresponding to later SA1 aggregate tables
createTable = '''  
  DROP TABLE IF EXISTS {0}.sa2_area;
  CREATE TABLE {0}.sa2_area AS
  SELECT sa2_name_2016, 
  string_agg(distinct(ssc_name),',') AS suburb, 
  string_agg(distinct(lga_name11), ', ') AS lga
  FROM  {0}.raw_indicators_hard
  WHERE sa2_name_2016 IN (SELECT sa2_name_2016 FROM abs_2011_irsd)
  GROUP BY sa2_name_2016
  ORDER BY sa2_name_2016 ASC;
  '''.format(uli_schema)
curs.execute(createTable)
conn.commit()


# create Suburb area linkage corresponding to later SA1 aggregate tables
createTable = '''  
  DROP TABLE IF EXISTS {0}.ssc_area;
  CREATE TABLE {0}.ssc_area AS
  SELECT DISTINCT(ssc_name) AS suburb, 
  string_agg(distinct(lga_name11), ', ') AS lga
  FROM  {0}.raw_indicators_hard
  GROUP BY ssc_name
  ORDER BY ssc_name ASC;
  '''.format(uli_schema)
curs.execute(createTable)
conn.commit()
  
# create aggregated raw liveability estimates for selected area
for type in ['hard','soft']:
  for area in ['mb_code_2016','sa1_maincode','sa2_name_2016','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.li_raw_{1}_{0} ; 
    CREATE TABLE {2}.li_raw_{1}_{0} AS
    SELECT {0},
      AVG(li_ci_est                   ) AS li_ci_est                    ,
      AVG(dd_nh1600m                  ) AS dd_nh1600m                   ,
      AVG(sc_nh1600m                  ) AS sc_nh1600m                   ,
      AVG(pos15000_access             ) AS pos15000_access              ,
      AVG(sa1_prop_affordablehousing  ) AS sa1_prop_affordablehousing   ,
      AVG(sa2_prop_live_work_sa3      ) AS sa2_prop_live_work_sa3       ,
      AVG(communitycentre_1000m       ) AS communitycentre_1000m        ,
      AVG(museumartgallery_3200m      ) AS museumartgallery_3200m       ,
      AVG(cinematheatre_3200m         ) AS cinematheatre_3200m          ,
      AVG(libraries_2014_1000m        ) AS libraries_2014_1000m         ,
      AVG(childcareoutofschool_1600m  ) AS childcareoutofschool_1600m   ,
      AVG(childcare_800m              ) AS childcare_800m               ,
      AVG(statesecondaryschools_1600m ) AS statesecondaryschools_1600m  ,
      AVG(stateprimaryschools_1600m   ) AS stateprimaryschools_1600m    ,
      AVG(agedcare_2012_1000m         ) AS agedcare_2012_1000m          ,
      AVG(communityhealthcentres_1000m) AS communityhealthcentres_1000m ,
      AVG(dentists_1000m              ) AS dentists_1000m               ,
      AVG(gp_clinics_1000m            ) AS gp_clinics_1000m             ,
      AVG(maternalchildhealth_1000m   ) AS maternalchildhealth_1000m    ,
      AVG(swimmingpools_1200m         ) AS swimmingpools_1200m          ,
      AVG(sport_1200m                 ) AS sport_1200m                  ,
      AVG(supermarkets_1000m          ) AS supermarkets_1000m           ,
      AVG(conveniencestores_1000m     ) AS conveniencestores_1000m      ,
      AVG(petrolstations_1000m        ) AS petrolstations_1000m         ,
      AVG(newsagents_1000m            ) AS newsagents_1000m             ,
      AVG(fishmeatpoultryshops_1600m  ) AS fishmeatpoultryshops_1600m   ,
      AVG(fruitvegeshops_1600m        ) AS fruitvegeshops_1600m         ,
      AVG(pharmacy_1000m              ) AS pharmacy_1000m               ,
      AVG(busstop2012_400m            ) AS busstop2012_400m             ,
      AVG(tramstops2012_600m          ) AS tramstops2012_600m           ,
      AVG(trainstations2012_800m      ) AS trainstations2012_800m       
      FROM {2}.area_indicators_{1}
      GROUP BY {0}
      ORDER BY {0} ASC;
    ALTER TABLE {2}.li_raw_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)
    curs.execute(createTable)
    conn.commit()
    print("Created raw {1} averages at {0} level for schema {2}".format(area,type,uli_schema))   
    
# create aggregated SD for raw liveability estimates for selected area
for type in ['hard','soft']:
  for area in ['mb_code_2016','sa1_maincode','sa2_name_2016','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.li_raw_sd_{1}_{0} ; 
    CREATE TABLE {2}.li_raw_sd_{1}_{0} AS
    SELECT {0},
      stddev_pop(li_ci_est                   ) AS sd_li_ci_est                   ,
      stddev_pop(dd_nh1600m                  ) AS sd_dd_nh1600m                  ,
      stddev_pop(sc_nh1600m                  ) AS sd_sc_nh1600m                  ,
      stddev_pop(pos15000_access             ) AS sd_pos15000_access             ,
      stddev_pop(sa1_prop_affordablehousing  ) AS sd_sa1_prop_affordablehousing  ,
      stddev_pop(sa2_prop_live_work_sa3      ) AS sd_sa2_prop_live_work_sa3      ,
      stddev_pop(communitycentre_1000m        ) AS sd_communitycentre_1000m        ,
      stddev_pop(museumartgallery_3200m       ) AS sd_museumartgallery_3200m       ,
      stddev_pop(cinematheatre_3200m          ) AS sd_cinematheatre_3200m          ,
      stddev_pop(libraries_2014_1000m         ) AS sd_libraries_2014_1000m         ,
      stddev_pop(childcareoutofschool_1600m   ) AS sd_childcareoutofschool_1600m   ,
      stddev_pop(childcare_800m               ) AS sd_childcare_800m               ,
      stddev_pop(statesecondaryschools_1600m  ) AS sd_statesecondaryschools_1600m  ,
      stddev_pop(stateprimaryschools_1600m    ) AS sd_stateprimaryschools_1600m    ,
      stddev_pop(agedcare_2012_1000m          ) AS sd_agedcare_2012_1000m          ,
      stddev_pop(communityhealthcentres_1000m ) AS sd_communityhealthcentres_1000m ,
      stddev_pop(dentists_1000m               ) AS sd_dentists_1000m               ,
      stddev_pop(gp_clinics_1000m             ) AS sd_gp_clinics_1000m             ,
      stddev_pop(maternalchildhealth_1000m    ) AS sd_maternalchildhealth_1000m    ,
      stddev_pop(swimmingpools_1200m          ) AS sd_swimmingpools_1200m          ,
      stddev_pop(sport_1200m                  ) AS sd_sport_1200m                  ,
      stddev_pop(supermarkets_1000m           ) AS sd_supermarkets_1000m           ,
      stddev_pop(conveniencestores_1000m      ) AS sd_conveniencestores_1000m      ,
      stddev_pop(petrolstations_1000m         ) AS sd_petrolstations_1000m         ,
      stddev_pop(newsagents_1000m             ) AS sd_newsagents_1000m             ,
      stddev_pop(fishmeatpoultryshops_1600m   ) AS sd_fishmeatpoultryshops_1600m   ,
      stddev_pop(fruitvegeshops_1600m         ) AS sd_fruitvegeshops_1600m         ,
      stddev_pop(pharmacy_1000m               ) AS sd_pharmacy_1000m               ,
      stddev_pop(busstop2012_400m             ) AS sd_busstop2012_400m             ,
      stddev_pop(tramstops2012_600m           ) AS sd_tramstops2012_600m           ,
      stddev_pop(trainstations2012_800m       ) AS sd_trainstations2012_800m       
      FROM  {2}.area_indicators_{1}
      GROUP BY {0}
      ORDER BY {0} ASC;
    ALTER TABLE {2}.li_raw_sd_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)    

    curs.execute(createTable)
    conn.commit()
    print("Created SD for raw {1} averages at {0} level for schema {2}".format(area,type,uli_schema))   


# create aggregated raw liveability range for selected area
for type in ['hard','soft']:
  for area in ['mb_code_2016','sa1_maincode','sa2_name_2016','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.li_range_{1}_{0} ; 
    CREATE TABLE {2}.li_range_{1}_{0} AS
    SELECT {0},
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
    createTable = '''DROP TABLE IF EXISTS {6}.clean_li_map_{0};
    CREATE TABLE {6}.clean_li_map_{0} AS
    SELECT {1}
    round({6}_lrh.walkability::numeric,1)  AS rh1,
    round({6}_lrh.daily_living::numeric,1) AS rh2,
    round({6}_lrh.dd_nh1600m::numeric,1)   AS rh3,
    round({6}_lrh.sc_nh1600m::numeric,1)   AS rh4,
    round({6}_lph.walkability::numeric,0)  AS ph1,
    round({6}_lph.daily_living::numeric,0) AS ph2,
    round({6}_lph.dd_nh1600m::numeric,0)   AS ph3,
    round({6}_lph.sc_nh1600m::numeric,0)   AS ph4,
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
    FROM {6}.li_raw_hard_{2}                    AS {6}_lrh 
    LEFT JOIN {6}.clean_li_percentiles_hard_{2} AS {6}_lph ON {6}_lrh.{2}  = {6}_lph.{2}
    LEFT JOIN {6}.li_range_hard_{2}             AS trh ON {6}_lrh.{2}  = trh.{2}
    LEFT JOIN {6}.li_most_hard_{2}              AS tmh ON {6}_lrh.{2}  = tmh.{2}
    LEFT JOIN {6}.li_raw_soft_{2}               AS {6}_lrs  ON {6}_lrh.{2}  = {6}_lrs.{2}
    LEFT JOIN {6}.clean_li_percentiles_soft_{2} AS {6}_lps  ON {6}_lrh.{2}  = {6}_lps.{2}
    LEFT JOIN {6}.li_range_soft_{2}             AS trs ON {6}_lrh.{2}  = trs.{2}
    LEFT JOIN {6}.li_most_soft_{2}              AS tms ON {6}_lrh.{2}  = tms.{2}
    LEFT JOIN {7}.clean_li_percentiles_hard_{2} AS {7}_lph ON {7}_lrh.{2}  = {7}_lph.{2}
    LEFT JOIN {7}.li_range_hard_{2}             AS trh ON {7}_lrh.{2}  = trh.{2}
    LEFT JOIN {7}.li_most_hard_{2}              AS tmh ON {7}_lrh.{2}  = tmh.{2}
    LEFT JOIN {7}.li_raw_soft_{2}               AS {7}_lrs  ON {7}_lrh.{2}  = {7}_lrs.{2}
    LEFT JOIN {7}.clean_li_percentiles_soft_{2} AS {7}_lps  ON {7}_lrh.{2}  = {7}_lps.{2}
    LEFT JOIN {7}.li_range_soft_{2}             AS trs ON {7}_lrh.{2}  = trs.{2}
    LEFT JOIN {7}.li_most_soft_{2}              AS tms ON {7}_lrh.{2}  = tms.{2}
    LEFT JOIN {8}.clean_li_percentiles_hard_{2} AS {8}_lph ON {8}_lrh.{2}  = {8}_lph.{2}
    LEFT JOIN {8}.li_range_hard_{2}             AS trh ON {8}_lrh.{2}  = trh.{2}
    LEFT JOIN {8}.li_most_hard_{2}              AS tmh ON {8}_lrh.{2}  = tmh.{2}
    LEFT JOIN {8}.li_raw_soft_{2}               AS {8}_lrs  ON {8}_lrh.{2}  = {8}_lrs.{2}
    LEFT JOIN {8}.clean_li_percentiles_soft_{2} AS {8}_lps  ON {8}_lrh.{2}  = {8}_lps.{2}
    LEFT JOIN {8}.li_range_soft_{2}             AS trs ON {8}_lrh.{2}  = trs.{2}
    LEFT JOIN {8}.li_most_soft_{2}              AS tms ON {8}_lrh.{2}  = tms.{2}
    {3}
    {4}
    {5};'''.format(short_area[areas.index(area)],area_strings[areas.index(area)],area,geom_tables[areas.index(area)],area_tables[areas.index(area)],community_code[areas.index(area)],uli_schema[0],uli_schema[1],uli_schema[2])
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
