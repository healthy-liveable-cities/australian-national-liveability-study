# Purpose: Create composite indicators
#          Note that daily and local living measures are calculated
#          in the 'neighbourhood indicators' script.
#          This is so they can be considered when running the script to 
#          identify exclusions.
# Author:  Carl Higgs 
# Date:    2020-01-13

import time
import psycopg2 
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Calculate composite indicators'

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

schema='ind_point'
points = sample_point_feature

# Calculate urban walkability index

# Note that for Highlife purposes, the calculation of the Walkability does not make sense 
# (ie. it is a relative measure for all other sample points in the city; 
# so where these are an arbitrary number of other building entry points, it just doesn't make sense

# Instead, we will link each with the closest Walkability calculated for national project and record distance of match


print("Creating urban walkability index... "),   
table = ['ind_walkability','wa']
sql = '''
DROP TABLE IF EXISTS {schema}.{table}; 
CREATE TABLE {schema}.{table} AS 
SELECT p.{points_id},
       dl.z_dl,
       sc.z_sc,
       dd.z_dd,
    dl.z_dl + sc.z_sc + dd.z_dd AS walkability_index 
  FROM {points} p 
    LEFT JOIN (SELECT {points_id}, 
                (dl_soft_1600m - AVG(dl_soft_1600m) 
                    OVER())
                    /stddev_pop(dl_soft_1600m) OVER() as z_dl 
              FROM {schema}.ind_daily_living) dl  ON dl.{points_id} = p.{points_id}
    LEFT JOIN (SELECT {points_id}, (sc_nh1600m - AVG(sc_nh1600m) OVER())
                  /stddev_pop(sc_nh1600m) OVER() as z_sc 
               FROM {schema}.sc_nh1600m) sc ON sc.{points_id} = p.{points_id}
    LEFT JOIN (SELECT {points_id}, (dd_nh1600m - AVG(dd_nh1600m) OVER())
                 /stddev_pop(dd_nh1600m) OVER() as z_dd 
               FROM {schema}.dd_nh1600m) dd ON dd.{points_id} = p.{points_id}
WHERE NOT EXISTS (SELECT 1 
                    FROM {schema}.excluded_parcels e
                   WHERE p.{points_id} = e.{points_id});
CREATE UNIQUE INDEX {table}_idx ON  {schema}.{table} ({points_id});
'''.format(table = table[0], 
           schema=schema,
           abbrev = table[1], 
           points=points,
           points_id = points_id)
curs.execute(sql)
conn.commit()
print(" Done.")


# Social Infrastructure Mix
table = 'ind_si_mix'
abbrev = 'si'
print("Creating Social Infrastructure Mix score... "),   

sql = '''
DROP TABLE IF EXISTS {schema}.{table};
CREATE TABLE IF NOT EXISTS {schema}.{table} AS
SELECT p.{points_id},
    (COALESCE(threshold_soft(nh_inds_distance.community_centre_hlc_2016_osm_2018, 1000),0) +
    COALESCE(threshold_soft(LEAST(array_min("museum_osm".distances),array_min("art_gallery_osm".distances)), 3200),0) +
    COALESCE(threshold_soft(LEAST(array_min("cinema_osm".distances),array_min("theatre_osm".distances)), 3200),0) +
    COALESCE(threshold_soft(array_min("libraries_2018".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("childcare_oshc_meet_2019".distances), 1600),0) +
    COALESCE(threshold_soft(array_min("childcare_all_meet_2019".distances), 800),0)  +
    COALESCE(threshold_soft(nh_inds_distance.schools_primary_all_gov, 1600),0) +
    COALESCE(threshold_soft(nh_inds_distance.schools_secondary_all_gov, 1600),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_aged_care_residential".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_pharmacy".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_mc_family_health".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_other_community_health_care".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_dentist".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_gp".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("public_swimming_pool_osm".distances), 1200),0) +
    COALESCE(threshold_soft(ind_os_distance.sport_distance_m, 1000),0)) AS si_mix
    FROM {points} p
    LEFT JOIN {schema}.nh_inds_distance ON p.{points_id} = {schema}.nh_inds_distance.{points_id}
    LEFT JOIN d_3200m_cl."museum_osm" ON p.{points_id} = d_3200m_cl."museum_osm".{points_id}
    LEFT JOIN d_3200m_cl."art_gallery_osm" ON p.{points_id} = d_3200m_cl."art_gallery_osm".{points_id}
    LEFT JOIN d_3200m_cl."cinema_osm" ON p.{points_id} = d_3200m_cl."cinema_osm".{points_id}
    LEFT JOIN d_3200m_cl."theatre_osm" ON p.{points_id} = d_3200m_cl."theatre_osm".{points_id}
    LEFT JOIN d_3200m_cl."libraries_2018" ON p.{points_id} = d_3200m_cl."libraries_2018".{points_id}
    LEFT JOIN d_3200m_cl."childcare_oshc_meet_2019" ON p.{points_id} = d_3200m_cl."childcare_oshc_meet_2019".{points_id}
    LEFT JOIN d_3200m_cl."childcare_all_meet_2019" ON p.{points_id} = d_3200m_cl."childcare_all_meet_2019".{points_id}
    LEFT JOIN d_3200m_cl."nhsd_2017_aged_care_residential" ON p.{points_id} = d_3200m_cl."nhsd_2017_aged_care_residential".{points_id}
    LEFT JOIN d_3200m_cl."nhsd_2017_pharmacy" ON p.{points_id} = d_3200m_cl."nhsd_2017_pharmacy".{points_id}
    LEFT JOIN d_3200m_cl."nhsd_2017_mc_family_health" ON p.{points_id} = d_3200m_cl."nhsd_2017_mc_family_health".{points_id}
    LEFT JOIN d_3200m_cl."nhsd_2017_other_community_health_care" ON p.{points_id} = d_3200m_cl."nhsd_2017_other_community_health_care".{points_id}
    LEFT JOIN d_3200m_cl."nhsd_2017_dentist" ON p.{points_id} = d_3200m_cl."nhsd_2017_dentist".{points_id}
    LEFT JOIN d_3200m_cl."nhsd_2017_gp" ON p.{points_id} = d_3200m_cl."nhsd_2017_gp".{points_id}
    LEFT JOIN d_3200m_cl."public_swimming_pool_osm" ON p.{points_id} = d_3200m_cl."public_swimming_pool_osm".{points_id}
    LEFT JOIN {schema}.ind_os_distance ON p.{points_id} = {schema}.ind_os_distance.{points_id};
    CREATE UNIQUE INDEX IF NOT EXISTS {table}_idx ON  {schema}.{table} ({points_id});
'''.format(points_id = points_id,
           points=points,
           schema=schema,
           table = table)
curs.execute(sql)
conn.commit()
print(" Done.")


# The Urban Liveability Index

# Note that for Highlife purposes, the calculation of the ULI does not make sense 
# (ie. it is a relative measure for all other sample points in the city; 
# so where these are an arbitrary number of other building entry points, it just doesn't make sense.
# We would ultimately expect little variation.

# Instead, we will link each with the closest ULI calculated for national project and record distance of match

# Read in indicator description matrix
ind_matrix = df_inds
uli = {}
for ind in ['dwelling_density','street_connectivity','walkability','pt_regular_400m','pos_large_400m','supermarket_1km']:
  suffix = ''
  if ind in ['walkability','pt_regular_400m','pos_large_400m','supermarket_1km']:
    suffix = '_soft'
  uli[ind] = '{}{}'.format(ind_matrix.loc[ind_matrix['ind_plain']==ind,'ind'].values[0].encode('utf8'),suffix)


# Restrict to indicators associated with study region
ind_matrix = ind_matrix[ind_matrix['ind']=='uli']
uli_locations = ind_matrix[ind_matrix['ind']=='uli']['locale'].iloc[0].encode('utf')
if locale not in uli_locations and uli_locations != '*':
  print("This location ('{locale}') is not marked for calculation of the Urban Liveability Index; check the indicator_setup file.".format(locale = locale))
  sys.exit()

# Define function to shape if variable is outlying  
createFunction = '''
  -- outlier limiting/compressing function
  -- if x < -2SD(x), scale up (hard knee upwards compression) to reach minimum by -3SD.
  -- if x > 2SD(x), scale up (hard knee downwards compression) to reach maximum by 3SD(x).
  
  CREATE OR REPLACE FUNCTION clean(var double precision,min_val double precision, max_val double precision, mean double precision, sd double precision) RETURNS double precision AS 
  $$
  DECLARE
  ll double precision := mean - 2*sd;
  ul double precision := mean + 2*sd;
  c  double precision :=  1*sd;
  BEGIN
    IF (min_val < ll-c) AND (var < ll) THEN 
      RETURN ll - c + c*(var - min_val)/(ll-min_val);
    ELSIF (max_val > ul+c) AND (var > ul) THEN 
      RETURN ul + c*(var - ul)/( max_val - ul );
    ELSE 
      RETURN var;
    END IF;
  END;
  $$
  LANGUAGE plpgsql
  RETURNS NULL ON NULL INPUT;
  '''
curs.execute(createFunction)
conn.commit()
print("Created custom function.")


# collate indicators for national liveability index

sql = '''
DROP TABLE IF EXISTS {schema}.uli_inds ; 
CREATE TABLE IF NOT EXISTS {schema}.uli_inds AS
SELECT p.{points_id},
    COALESCE(sc_nh1600m,0) AS sc_nh1600m,
    COALESCE(dd_nh1600m,0) AS dd_nh1600m,
   (COALESCE(threshold_soft(nh_inds_distance.community_centre_hlc_2016_osm_2018, 1000),0) +
    COALESCE(threshold_soft(LEAST(array_min("museum_osm".distances),array_min("art_gallery_osm".distances)), 3200),0) +
    COALESCE(threshold_soft(LEAST(array_min("cinema_osm".distances),array_min("theatre_osm".distances)), 3200),0) +
    COALESCE(threshold_soft(array_min("libraries_2018".distances), 1000),0))/4.0 AS community_culture_leisure ,
   (COALESCE(threshold_soft(array_min("childcare_oshc_meet_2019".distances), 1600),0) +
    COALESCE(threshold_soft(array_min("childcare_all_meet_2019".distances), 800),0))/2.0 AS early_years,
   (COALESCE(threshold_soft(nh_inds_distance.schools_primary_all_gov, 1600),0) +
    COALESCE(threshold_soft(nh_inds_distance.schools_secondary_all_gov, 1600),0))/2.0 AS education ,
   (COALESCE(threshold_soft(array_min("nhsd_2017_aged_care_residential".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_pharmacy".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_mc_family_health".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_other_community_health_care".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_dentist".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("nhsd_2017_gp".distances), 1000),0))/6.0 AS health_services ,
   (COALESCE(threshold_soft(array_min("public_swimming_pool_osm".distances), 1200),0) +
    COALESCE(threshold_soft(ind_os_distance.sport_distance_m, 1000),0))/2.0 AS sport_rec,
   (COALESCE(threshold_soft(array_min("fruit_veg_osm".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("meat_seafood_osm".distances), 3200),0) +
    COALESCE(threshold_soft(array_min("supermarket_osm".distances), 1000),0))/3.0 AS food,    
   (COALESCE(threshold_soft(array_min("convenience_osm".distances), 1000),0) +
    COALESCE(threshold_soft(array_min("newsagent_osm".distances), 3200),0) +
    COALESCE(threshold_soft(array_min("petrolstation_osm".distances), 1000),0))/3.0 AS convenience,         
    COALESCE(threshold_soft(nh_inds_distance.gtfs_20191008_20191205_pt_0020,400),0) AS pt_regular_400m,
    COALESCE(threshold_soft(ind_os_distance.pos_15k_sqm_distance_m,400),0) AS pos_large_400m,
    -- we coalesce 30:40 measures to 0, as nulls mean no one is in bottom two housing quintiles - really 0/0 implies 0% in this context
    -- noting that null is not acceptable.  This should be discussed, but is workable for now.
    -- Later, we reverse polarity of 30 40 measure
    COALESCE(pct_30_40_housing,0) AS abs_30_40,
    COALESCE(pct_live_work_local_area,0) AS abs_live_sa1_work_sa3
FROM {points} p
LEFT JOIN area_linkage a ON p.mb_code_2016 = a.mb_code_2016
LEFT JOIN (SELECT DISTINCT({points_id}) FROM {schema}.excluded_parcels) e ON p.{points_id} = e.{points_id}
LEFT JOIN {schema}.nh_inds_distance ON p.{points_id} = {schema}.nh_inds_distance.{points_id}
LEFT JOIN {schema}.sc_nh1600m ON p.{points_id} = {schema}.sc_nh1600m.{points_id}
LEFT JOIN {schema}.dd_nh1600m ON p.{points_id} = {schema}.dd_nh1600m.{points_id}
LEFT JOIN {schema}.ind_os_distance ON p.{points_id} = {schema}.ind_os_distance.{points_id}
LEFT JOIN ind_sa1.abs_indicators abs ON a.sa1_7digitcode_2016 = abs.sa1_7digitcode_2016::text
LEFT JOIN d_3200m_cl."fruit_veg_osm" ON p.{points_id} = d_3200m_cl."fruit_veg_osm".{points_id}
LEFT JOIN d_3200m_cl."meat_seafood_osm" ON p.{points_id} = d_3200m_cl."meat_seafood_osm".{points_id}
LEFT JOIN d_3200m_cl."supermarket_osm" ON p.{points_id} = d_3200m_cl."supermarket_osm".{points_id}
LEFT JOIN d_3200m_cl."convenience_osm" ON p.{points_id} = d_3200m_cl."convenience_osm".{points_id}
LEFT JOIN d_3200m_cl."newsagent_osm" ON p.{points_id} = d_3200m_cl."newsagent_osm".{points_id}
LEFT JOIN d_3200m_cl."petrolstation_osm" ON p.{points_id} = d_3200m_cl."petrolstation_osm".{points_id}
LEFT JOIN d_3200m_cl."museum_osm" ON p.{points_id} = d_3200m_cl."museum_osm".{points_id}
LEFT JOIN d_3200m_cl."art_gallery_osm" ON p.{points_id} = d_3200m_cl."art_gallery_osm".{points_id}
LEFT JOIN d_3200m_cl."cinema_osm" ON p.{points_id} = d_3200m_cl."cinema_osm".{points_id}
LEFT JOIN d_3200m_cl."theatre_osm" ON p.{points_id} = d_3200m_cl."theatre_osm".{points_id}
LEFT JOIN d_3200m_cl."libraries_2018" ON p.{points_id} = d_3200m_cl."libraries_2018".{points_id}
LEFT JOIN d_3200m_cl."childcare_oshc_meet_2019" ON p.{points_id} = d_3200m_cl."childcare_oshc_meet_2019".{points_id}
LEFT JOIN d_3200m_cl."childcare_all_meet_2019" ON p.{points_id} = d_3200m_cl."childcare_all_meet_2019".{points_id}
LEFT JOIN d_3200m_cl."nhsd_2017_aged_care_residential" ON p.{points_id} = d_3200m_cl."nhsd_2017_aged_care_residential".{points_id}
LEFT JOIN d_3200m_cl."nhsd_2017_pharmacy" ON p.{points_id} = d_3200m_cl."nhsd_2017_pharmacy".{points_id}
LEFT JOIN d_3200m_cl."nhsd_2017_mc_family_health" ON p.{points_id} = d_3200m_cl."nhsd_2017_mc_family_health".{points_id}
LEFT JOIN d_3200m_cl."nhsd_2017_other_community_health_care" ON p.{points_id} = d_3200m_cl."nhsd_2017_other_community_health_care".{points_id}
LEFT JOIN d_3200m_cl."nhsd_2017_dentist" ON p.{points_id} = d_3200m_cl."nhsd_2017_dentist".{points_id}
LEFT JOIN d_3200m_cl."nhsd_2017_gp" ON p.{points_id} = d_3200m_cl."nhsd_2017_gp".{points_id}
LEFT JOIN d_3200m_cl."public_swimming_pool_osm" ON p.{points_id} = d_3200m_cl."public_swimming_pool_osm".{points_id}
WHERE e.{points_id} IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ix_uli_inds ON  {schema}.uli_inds ({points_id});
'''.format(points_id = points_id,points=points,schema=schema)
curs.execute(sql)
conn.commit()
print("Created liveability indicator table uli_inds.")

# The below uses our custom clean function, drawing on (indicator, min, max, mean, sd)
sql = '''
DROP TABLE IF EXISTS {schema}.uli_inds_clean ; 
CREATE TABLE {schema}.uli_inds_clean AS
SELECT i.{points_id},
       clean(i.sc_nh1600m               , s.sc_nh1600m[1]               , s.sc_nh1600m[2]                , s.sc_nh1600m[3]               , s.sc_nh1600m[4]               ) AS sc_nh1600m               ,
       clean(i.dd_nh1600m               , s.dd_nh1600m[1]               , s.dd_nh1600m[2]                , s.dd_nh1600m[3]               , s.dd_nh1600m[4]               ) AS dd_nh1600m               ,
       clean(i.community_culture_leisure, s.community_culture_leisure[1], s.community_culture_leisure[2] , s.community_culture_leisure[3], s.community_culture_leisure[4]) AS community_culture_leisure,
       clean(i.early_years              , s.early_years[1]              , s.early_years[2]               , s.early_years[3]              , s.early_years[4]              ) AS early_years              ,
       clean(i.education                , s.education[1]                , s.education[2]                 , s.education[3]                , s.education[4]                ) AS education                ,
       clean(i.health_services          , s.health_services[1]          , s.health_services[2]           , s.health_services[3]          , s.health_services[4]          ) AS health_services          , 
       clean(i.sport_rec                , s.sport_rec[1]                , s.sport_rec[2]                 , s.sport_rec[3]                , s.sport_rec[4]                ) AS sport_rec                , 
       clean(i.food                     , s.food[1]                     , s.food[2]                      , s.food[3]                     , s.food[4]                     ) AS food                     , 
       clean(i.convenience              , s.convenience[1]              , s.convenience[2]               , s.convenience[3]              , s.convenience[4]              ) AS convenience              , 
       clean(i.pt_regular_400m          , s.pt_regular_400m[1]          , s.pt_regular_400m[2]           , s.pt_regular_400m[3]          , s.pt_regular_400m[4]          ) AS pt_regular_400m          , 
       clean(i.pos_large_400m           , s.pos_large_400m[1]           , s.pos_large_400m[2]            , s.pos_large_400m[3]           , s.pos_large_400m[4]           ) AS pos_large_400m           , 
       clean(i.abs_30_40                , s.abs_30_40[1]                , s.abs_30_40[2]                 , s.abs_30_40[3]                , s.abs_30_40[4]                ) AS abs_30_40                , 
       clean(i.abs_live_sa1_work_sa3    , s.abs_live_sa1_work_sa3[1]    , s.abs_live_sa1_work_sa3[2]     , s.abs_live_sa1_work_sa3[3]    , s.abs_live_sa1_work_sa3[4]    ) AS abs_live_sa1_work_sa3     
FROM {schema}.uli_inds i,
(SELECT ARRAY[MIN(sc_nh1600m               ),MAX(sc_nh1600m               ),AVG(sc_nh1600m               ),STDDEV(sc_nh1600m               )] AS sc_nh1600m               ,
        ARRAY[MIN(dd_nh1600m               ),MAX(dd_nh1600m               ),AVG(dd_nh1600m               ),STDDEV(dd_nh1600m               )] AS dd_nh1600m               ,
        ARRAY[MIN(community_culture_leisure),MAX(community_culture_leisure),AVG(community_culture_leisure),STDDEV(community_culture_leisure)] AS community_culture_leisure,
        ARRAY[MIN(early_years              ),MAX(early_years              ),AVG(early_years              ),STDDEV(early_years              )] AS early_years              ,
        ARRAY[MIN(education                ),MAX(education                ),AVG(education                ),STDDEV(education                )] AS education                ,
        ARRAY[MIN(health_services          ),MAX(health_services          ),AVG(health_services          ),STDDEV(health_services          )] AS health_services          ,
        ARRAY[MIN(sport_rec                ),MAX(sport_rec                ),AVG(sport_rec                ),STDDEV(sport_rec                )] AS sport_rec                ,
        ARRAY[MIN(food                     ),MAX(food                     ),AVG(food                     ),STDDEV(food                     )] AS food                     ,
        ARRAY[MIN(convenience              ),MAX(convenience              ),AVG(convenience              ),STDDEV(convenience              )] AS convenience              ,
        ARRAY[MIN(pt_regular_400m          ),MAX(pt_regular_400m          ),AVG(pt_regular_400m          ),STDDEV(pt_regular_400m          )] AS pt_regular_400m          ,
        ARRAY[MIN(pos_large_400m           ),MAX(pos_large_400m           ),AVG(pos_large_400m           ),STDDEV(pos_large_400m           )] AS pos_large_400m           ,
        ARRAY[MIN(abs_30_40                ),MAX(abs_30_40                ),AVG(abs_30_40                ),STDDEV(abs_30_40                )] AS abs_30_40                ,
        ARRAY[MIN(abs_live_sa1_work_sa3    ),MAX(abs_live_sa1_work_sa3    ),AVG(abs_live_sa1_work_sa3    ),STDDEV(abs_live_sa1_work_sa3    )] AS abs_live_sa1_work_sa3    
 FROM {schema}.uli_inds) s;
ALTER TABLE {schema}.uli_inds_clean ADD PRIMARY KEY ({points_id});
  '''.format(points_id = points_id,schema=schema)
curs.execute(sql)
conn.commit()
print("Created table 'uli_inds_clean'")


sql = '''
-- Note that in this normalisation stage, indicator polarity is adjusted for: ABS 30:40 measure has values substracted from 100, whilst positive indicators have them added.
DROP TABLE IF EXISTS {schema}.uli_inds_norm ; 
CREATE TABLE {schema}.uli_inds_norm AS    
SELECT c.{points_id},
       100 + 10 * (c.sc_nh1600m               - s.sc_nh1600m[1]               ) / s.sc_nh1600m[2]                ::double precision AS sc_nh1600m               ,
       100 + 10 * (c.dd_nh1600m               - s.dd_nh1600m[1]               ) / s.dd_nh1600m[2]                ::double precision AS dd_nh1600m               ,
       100 + 10 * (c.community_culture_leisure- s.community_culture_leisure[1]) / s.community_culture_leisure[2] ::double precision AS community_culture_leisure,
       100 + 10 * (c.early_years              - s.early_years[1]              ) / s.early_years[2]               ::double precision AS early_years              ,
       100 + 10 * (c.education                - s.education[1]                ) / s.education[2]                 ::double precision AS education                ,
       100 + 10 * (c.health_services          - s.health_services[1]          ) / s.health_services[2]           ::double precision AS health_services          ,
       100 + 10 * (c.sport_rec                - s.sport_rec[1]                ) / s.sport_rec[2]                 ::double precision AS sport_rec                ,
       100 + 10 * (c.food                     - s.food[1]                     ) / s.food[2]                      ::double precision AS food                     ,
       100 + 10 * (c.convenience              - s.convenience[1]              ) / s.convenience[2]               ::double precision AS convenience              ,
       100 + 10 * (c.pt_regular_400m          - s.pt_regular_400m[1]          ) / s.pt_regular_400m[2]           ::double precision AS pt_regular_400m          ,
       100 + 10 * (c.pos_large_400m           - s.pos_large_400m[1]           ) / s.pos_large_400m[2]            ::double precision AS pos_large_400m           ,
       100 - 10 * (c.abs_30_40                - s.abs_30_40[1]                ) / s.abs_30_40[2]                 ::double precision AS abs_30_40                ,
       100 + 10 * (c.abs_live_sa1_work_sa3    - s.abs_live_sa1_work_sa3[1]    ) / s.abs_live_sa1_work_sa3[2]     ::double precision AS abs_live_sa1_work_sa3    
FROM {schema}.uli_inds_clean c,
(SELECT ARRAY[AVG(sc_nh1600m               ),CASE WHEN STDDEV(sc_nh1600m                ) = 0 THEN 1 ELSE  STDDEV(sc_nh1600m                ) END] AS sc_nh1600m               ,
        ARRAY[AVG(dd_nh1600m               ),CASE WHEN STDDEV(dd_nh1600m                ) = 0 THEN 1 ELSE  STDDEV(dd_nh1600m                ) END] AS dd_nh1600m               ,
        ARRAY[AVG(community_culture_leisure),CASE WHEN STDDEV(community_culture_leisure ) = 0 THEN 1 ELSE  STDDEV(community_culture_leisure ) END] AS community_culture_leisure,
        ARRAY[AVG(early_years              ),CASE WHEN STDDEV(early_years               ) = 0 THEN 1 ELSE  STDDEV(early_years               ) END] AS early_years              ,
        ARRAY[AVG(education                ),CASE WHEN STDDEV(education                 ) = 0 THEN 1 ELSE  STDDEV(education                 ) END] AS education                ,
        ARRAY[AVG(health_services          ),CASE WHEN STDDEV(health_services           ) = 0 THEN 1 ELSE  STDDEV(health_services           ) END] AS health_services          ,
        ARRAY[AVG(sport_rec                ),CASE WHEN STDDEV(sport_rec                 ) = 0 THEN 1 ELSE  STDDEV(sport_rec                 ) END] AS sport_rec                ,
        ARRAY[AVG(food                     ),CASE WHEN STDDEV(food                      ) = 0 THEN 1 ELSE  STDDEV(food                      ) END] AS food                     ,
        ARRAY[AVG(convenience              ),CASE WHEN STDDEV(convenience               ) = 0 THEN 1 ELSE  STDDEV(convenience               ) END] AS convenience              ,
        ARRAY[AVG(pt_regular_400m          ),CASE WHEN STDDEV(pt_regular_400m           ) = 0 THEN 1 ELSE  STDDEV(pt_regular_400m           ) END] AS pt_regular_400m          ,
        ARRAY[AVG(pos_large_400m           ),CASE WHEN STDDEV(pos_large_400m            ) = 0 THEN 1 ELSE  STDDEV(pos_large_400m            ) END] AS pos_large_400m           ,
        ARRAY[AVG(abs_30_40                ),CASE WHEN STDDEV(abs_30_40                 ) = 0 THEN 1 ELSE  STDDEV(abs_30_40                 ) END] AS abs_30_40                ,
        ARRAY[AVG(abs_live_sa1_work_sa3    ),CASE WHEN STDDEV(abs_live_sa1_work_sa3     ) = 0 THEN 1 ELSE  STDDEV(abs_live_sa1_work_sa3     ) END] AS abs_live_sa1_work_sa3    
 FROM {schema}.uli_inds_clean) s;
ALTER TABLE {schema}.uli_inds_norm ADD PRIMARY KEY ({points_id});
'''.format(points_id = points_id,points=points,schema=schema)

curs.execute(sql)
conn.commit()
print("Created table 'uli_inds_norm', a table of MPI-normalised indicators.")
 
sql = ''' 
-- 2. Create ULI
-- rowmean*(1-(rowsd(z_j)/rowmean(z_j))^2) AS mpi_est_j
DROP TABLE IF EXISTS {schema}.uli ; 
CREATE TABLE {schema}.uli AS
SELECT {points_id}, 
       AVG(val) AS mean, 
       stddev_pop(val) AS sd, 
       stddev_pop(val)/AVG(val) AS cv, 
       AVG(val)-(stddev_pop(val)^2)/AVG(val) AS uli 
FROM (SELECT {points_id}, 
             unnest(array[sc_nh1600m               ,
                          dd_nh1600m               ,
                          community_culture_leisure,
                          early_years              ,
                          education                ,
                          health_services          ,
                          sport_rec                ,
                          food                     ,
                          convenience              ,
                          pt_regular_400m          ,
                          pos_large_400m           ,
                          abs_30_40                ,
                          abs_live_sa1_work_sa3    
                          ]) as val 
      FROM {schema}.uli_inds_norm ) alias
GROUP BY {points_id};
ALTER TABLE {schema}.uli ADD PRIMARY KEY ({points_id});
'''.format(points_id = points_id,points=points,schema=schema)

curs.execute(sql)
conn.commit()
print("Created table 'uli', containing parcel level urban liveability index estimates, along with its required summary ingredients (mean, sd, coefficient of variation).")

# output to completion log    
script_running_log(script, task, start)
