# Script:  19_urban_liveability_index.py
# Purpose: Create parcel indicators for national liveability project
# Author:  Carl Higgs 
# Date:    20180910

#  Postgresql MPI implementation steps for i indicators across j parcels
#  De Muro P., Mazziotta M., Pareto A. (2011), "Composite Indices of Development and Poverty: An Application to MDGs", Social Indicators Research, Volume 104, Number 1, pp. 1-18.
#  Vidoli, F., Fusco, E. Compind: Composite Indicators Functions, Version 1.1.2, 2016 
#  Adapted for postgresql by Carl Higgs, 4/4/2017

import time
import psycopg2 
from sqlalchemy import create_engine
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'calculate urban liveability index (ULI) for {}'.format(locale)


# Read in indicator description matrix
ind_matrix = df_inds
uli = {}
for ind in ['dwelling_density','street_connectivity','walkability','pt_freq_400m','pos_large_400m','supermarket_1km']:
  suffix = ''
  if ind in ['walkability','pt_freq_400m','pos_large_400m','supermarket_1km']:
    suffix = '_soft'
  uli[ind] = '{}{}'.format(ind_matrix.loc[ind_matrix['ind_plain']==ind,'ind'].values[0].encode('utf8'),suffix)


# Restrict to indicators associated with study region
ind_matrix = ind_matrix[ind_matrix['ind']=='uli']
uli_locations = ind_matrix[ind_matrix['ind']=='uli']['locale'].iloc[0].encode('utf')
if locale not in uli_locations and uli_locations != '*':
  print("This location ('{locale}') is not marked for calculation of the Urban Liveability Index; check the ind_study_region_matrix file.".format(locale = locale))
  sys.exit()

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

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

# import affordable housing indicator
affordable_housing = pandas.read_csv('D:/ABS/data/2016/abs_liveability/housing_3040_sa1_all_20190712.csv', index_col=0)
affordable_housing.to_sql('abs_ind_30_40', con=engine, if_exists='replace')

# collate indicators for national liveability index

sql = '''
DROP TABLE IF EXISTS uli_inds ; 
CREATE TABLE uli_inds AS
SELECT p.{id},
        COALESCE(p.{street_connectivity},0) AS sc_nh1600m,
        COALESCE(p.{dwelling_density},0) AS dd_nh1600m,
        (COALESCE(threshold_soft(d."dist_m_museum_osm"                            , 3200),0) +
         COALESCE(threshold_soft(LEAST(d."dist_m_cinema_osm",d."dist_m_theatre_osm"), 3200),0) +
         COALESCE(threshold_soft(d."dist_m_libraries_2018"                        , 1000),0))/4.0 AS community_culture_leisure ,
        (COALESCE(threshold_soft(d."dist_m_childcare_oshc_meet_2019"              , 1600),0) +
         COALESCE(threshold_soft(d."dist_m_childcare_all_meet_2019"               , 800),0))/2.0 AS early_years,
        (COALESCE(threshold_soft(d."dist_m_P_12_Schools_gov_2018"                 , 1600),0) +
         COALESCE(threshold_soft(d."dist_m_secondary_schools2018"                 , 1600),0))/2.0 AS education ,
        (COALESCE(threshold_soft(d."dist_m_nhsd_2017_aged_care_residential"       , 1000),0) +
         COALESCE(threshold_soft(d."dist_m_nhsd_2017_pharmacy"                    , 1000),0) +
         COALESCE(threshold_soft(d."dist_m_nhsd_2017_mc_family_health"            , 1000),0) +
         COALESCE(threshold_soft(d."dist_m_nhsd_2017_dentist"                     , 1000),0) +
         COALESCE(threshold_soft(d."dist_m_nhsd_2017_gp"                          , 1000),0))/5.0 AS health_services ,
        (COALESCE(threshold_soft(d."dist_m_swimming_pool_osm"                     , 1200),0) +
         COALESCE(threshold_soft(o."sport_distance_m"                             , 1000),0))/2.0 AS sport_rec,
        (COALESCE(threshold_soft(d."dist_m_fruit_veg_osm"                         , 1000),0) +
         COALESCE(threshold_soft(d."dist_m_meat_seafood_osm"                      , 3200),0) +
         COALESCE(threshold_soft(d."dist_m_supermarket_osm"                       , 1000),0))/3.0 AS food,    
        (COALESCE(threshold_soft(d."dist_m_convenience_osm"                       , 1000),0) +
         COALESCE(threshold_soft(d."dist_m_newsagent_osm"                         , 3200),0) +
         COALESCE(threshold_soft(d."dist_m_petrolstation_osm"                     , 1000),0))/3.0 AS convenience,         
        COALESCE({pt_freq_400m},0) AS pt_regular_400m,
        COALESCE({pos_large_400m},0) AS pos_large_400m,
        -- we coalesce 30:40 measures to 0, as nulls mean no one is in bottom two housing quintiles - really 0/0 implies 0% in this context
        -- noting that null is not acceptable.  This should be discussed, but is workable for now.
        -- Later, we reverse polarity of 30 40 measure
        COALESCE(pcent_30_40,0) AS abs_30_40,
        COALESCE(pct_live_work_local_area,0) AS abs_live_sa1_work_sa3
FROM parcel_indicators p
LEFT JOIN area_linkage a USING (mb_code_2016)
LEFT JOIN dest_closest_indicators d USING ({id})
LEFT JOIN ind_os_distance o USING ({id})
LEFT JOIN abs_ind_30_40 h ON a.sa1_7digitcode_2016 = h.sa1_7digitcode_2016::text
LEFT JOIN live_sa1_work_sa3 l ON a.sa1_7digitcode_2016 = l.sa1_7digitcode_2016::text
WHERE p.exclude IS NULL;
ALTER TABLE uli_inds ADD PRIMARY KEY ({id});
  '''.format(id = points_id, 
             street_connectivity = uli['street_connectivity'],
             dwelling_density    = uli['dwelling_density'],
             supermarket_1km     = uli['supermarket_1km'],
             pt_freq_400m        = uli['pt_freq_400m'],
             pos_large_400m      = uli['pos_large_400m'])

curs.execute(sql)
conn.commit()
print("Created liveability indicator table uli_inds.")

# The below uses our custom clean function, drawing on (indicator, min, max, mean, sd)
sql = '''
DROP TABLE IF EXISTS uli_inds_clean ; 
CREATE TABLE uli_inds_clean AS
SELECT i.{id},
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
FROM uli_inds i,
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
 FROM uli_inds) s;
ALTER TABLE uli_inds_clean ADD PRIMARY KEY ({id});
  '''.format(id = points_id)
curs.execute(sql)
conn.commit()
print("Created table 'uli_inds_clean'")


sql = '''
-- Note that in this normalisation stage, indicator polarity is adjusted for: ABS 30:40 measure has values substracted from 100, whilst positive indicators have them added.
DROP TABLE IF EXISTS uli_inds_norm ; 
CREATE TABLE uli_inds_norm AS    
SELECT c.{id},
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
FROM uli_inds_clean c,
(SELECT ARRAY[AVG(sc_nh1600m               ),STDDEV(sc_nh1600m               )] AS sc_nh1600m               ,
        ARRAY[AVG(dd_nh1600m               ),STDDEV(dd_nh1600m               )] AS dd_nh1600m               ,
        ARRAY[AVG(community_culture_leisure),STDDEV(community_culture_leisure)] AS community_culture_leisure,
        ARRAY[AVG(early_years              ),STDDEV(early_years              )] AS early_years              ,
        ARRAY[AVG(education                ),STDDEV(education                )] AS education                ,
        ARRAY[AVG(health_services          ),STDDEV(health_services          )] AS health_services          ,
        ARRAY[AVG(sport_rec                ),STDDEV(sport_rec                )] AS sport_rec                ,
        ARRAY[AVG(food                     ),STDDEV(food                     )] AS food                     ,
        ARRAY[AVG(convenience              ),STDDEV(convenience              )] AS convenience              ,
        ARRAY[AVG(pt_regular_400m          ),STDDEV(pt_regular_400m          )] AS pt_regular_400m          ,
        ARRAY[AVG(pos_large_400m           ),STDDEV(pos_large_400m           )] AS pos_large_400m           ,
        ARRAY[AVG(abs_30_40                ),STDDEV(abs_30_40                )] AS abs_30_40                ,
        ARRAY[AVG(abs_live_sa1_work_sa3    ),STDDEV(abs_live_sa1_work_sa3    )] AS abs_live_sa1_work_sa3    
 FROM uli_inds_clean) s;
ALTER TABLE uli_inds_norm ADD PRIMARY KEY ({id});
'''.format(id = points_id)

curs.execute(sql)
conn.commit()
print("Created table 'uli_inds_norm', a table of MPI-normalised indicators.")
 
sql = ''' 
-- 2. Create ULI
-- rowmean*(1-(rowsd(z_j)/rowmean(z_j))^2) AS mpi_est_j
DROP TABLE IF EXISTS uli ; 
CREATE TABLE uli AS
SELECT {id}, 
       AVG(val) AS mean, 
       stddev_pop(val) AS sd, 
       stddev_pop(val)/AVG(val) AS cv, 
       AVG(val)-(stddev_pop(val)^2)/AVG(val) AS uli 
FROM (SELECT {id}, 
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
      FROM uli_inds_norm ) alias
GROUP BY {id};
ALTER TABLE uli ADD PRIMARY KEY ({id});
'''.format(id = points_id)

curs.execute(sql)
conn.commit()
print("Created table 'uli', containing parcel level urban liveability index estimates, along with its required summary ingredients (mean, sd, coefficient of variation).")


sql  = '''
-- Add a ULI column if it doesn't already exist to the parcel indicators table
-- and update it with the ULI values for those parcels
ALTER TABLE parcel_indicators ADD COLUMN IF NOT EXISTS uli double precision;
UPDATE parcel_indicators p
   SET uli = u.uli
  FROM uli u
 WHERE p.{id} = u.{id};
'''.format(id = points_id)

curs.execute(sql)
conn.commit()
print("Replaced table 'parcel_indicators' with a new version, containing the ULI")

# output to completion log    
script_running_log(script, task, start)
  
