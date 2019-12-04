# Purpose: Create ad hoc indicators for 21 city scorecards; script should be finalised later
# Author:  Carl Higgs 
# Date:    20191204


import time
import psycopg2 
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'calculate final indicators for 21 city scorecards'

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))
# Calculate urban walkability index
print("Creating urban walkability index... "),   
table = ['ind_walkability','wa']
sql = '''
DROP TABLE IF EXISTS {table}; 
CREATE TABLE {table} AS 
SELECT p.{id},
       dl.z_dl,
       sc.z_sc,
       dd.z_dd,
    dl.z_dl + sc.z_sc + dd.z_dd AS walkability_index 
  FROM parcel_dwellings p 
    LEFT JOIN (SELECT {id}, 
                (dl_soft_1600m - AVG(dl_soft_1600m) 
                    OVER())
                    /stddev_pop(dl_soft_1600m) OVER() as z_dl 
              FROM ind_daily_living) dl  ON dl.{id} = p.{id}
    LEFT JOIN (SELECT {id}, (sc_nh1600m - AVG(sc_nh1600m) OVER())
                  /stddev_pop(sc_nh1600m) OVER() as z_sc 
               FROM sc_nh1600m) sc ON sc.{id} = p.{id}
    LEFT JOIN (SELECT {id}, (dd_nh1600m - AVG(dd_nh1600m) OVER())
                 /stddev_pop(dd_nh1600m) OVER() as z_dd 
               FROM dd_nh1600m) dd ON dd.{id} = p.{id}
WHERE NOT EXISTS (SELECT 1 
                    FROM excluded_parcels e
                   WHERE p.{id} = e.{id});
CREATE UNIQUE INDEX {table}_idx ON  {table} ({id});
'''.format(table = table[0], 
           abbrev = table[1], 
           id = points_id)
curs.execute(sql)
conn.commit()
print(" Done.")

# The Urban Liveability Index


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
# Note - we coalesce null values to 0.0001, as if any variable has an average of zero 
# which might arise if no one has access to frequent pt, for example, then
# that will result in a division by zero error when normalising the data
sql = '''
DROP TABLE IF EXISTS uli_inds ; 
CREATE TABLE IF NOT EXISTS uli_inds AS
SELECT p.{id},
    COALESCE(sc_nh1600m,0.0001) AS sc_nh1600m,
    COALESCE(dd_nh1600m,0.0001) AS dd_nh1600m,
   (COALESCE(threshold_soft(nh_inds_distance.community_centre_hlc_2016_osm_2018, 1000),0.0001) +
    COALESCE(threshold_soft(LEAST(array_min("museum_osm".distances),array_min("art_gallery_osm".distances)), 3200),0.0001) +
    COALESCE(threshold_soft(LEAST(array_min("cinema_osm".distances),array_min("theatre_osm".distances)), 3200),0.0001) +
    COALESCE(threshold_soft(array_min("libraries".distances), 1000),0.0001))/4.0 AS community_culture_leisure ,
   (COALESCE(threshold_soft(array_min("childcare_oshc_meet".distances), 1600),0.0001) +
    COALESCE(threshold_soft(array_min("childcare_all_meet".distances), 800),0.0001))/2.0 AS early_years,
   (COALESCE(threshold_soft(nh_inds_distance.schools_primary_all_gov, 1600),0.0001) +
    COALESCE(threshold_soft(nh_inds_distance.schools_primary_all_gov, 1600),0.0001))/2.0 AS education ,
   (COALESCE(threshold_soft(array_min("nhsd_2017_aged_care_residential".distances), 1000),0.0001) +
    COALESCE(threshold_soft(array_min("nhsd_2017_pharmacy".distances), 1000),0.0001) +
    COALESCE(threshold_soft(array_min("nhsd_2017_mc_family_health".distances), 1000),0.0001) +
    COALESCE(threshold_soft(array_min("nhsd_2017_other_community_health_care".distances), 1000),0.0001) +
    COALESCE(threshold_soft(array_min("nhsd_2017_dentist".distances), 1000),0.0001) +
    COALESCE(threshold_soft(array_min("nhsd_2017_gp".distances), 1000),0.0001))/6.0 AS health_services ,
   (COALESCE(threshold_soft(array_min("public_swimming_pool_osm".distances), 1200),0.0001) +
    COALESCE(threshold_soft(ind_os_distance.sport_distance_m, 1000),0.0001))/2.0 AS sport_rec,
   (COALESCE(threshold_soft(array_min("fruit_veg_osm".distances), 1000),0.0001) +
    COALESCE(threshold_soft(array_min("meat_seafood_osm".distances), 3200),0.0001) +
    COALESCE(threshold_soft(array_min("supermarket_osm".distances), 1000),0.0001))/3.0 AS food,    
   (COALESCE(threshold_soft(array_min("convenience_osm".distances), 1000),0.0001) +
    COALESCE(threshold_soft(array_min("newsagent_osm".distances), 3200),0.0001) +
    COALESCE(threshold_soft(array_min("petrolstation_osm".distances), 1000),0.0001))/3.0 AS convenience,         
    COALESCE(threshold_soft(gtfs_20191008_20191205_frequent_pt_0030,400),0.0001) AS pt_regular_400m,
    COALESCE(threshold_soft(ind_os_distance.pos_15k_sqm_distance_m,400),0.0001) AS pos_large_400m,
    -- we coalesce 30:40 measures to 0, as nulls mean no one is in bottom two housing quintiles - really 0/0 implies 0% in this context
    -- noting that null is not acceptable.  This should be discussed, but is workable for now.
    -- Later, we reverse polarity of 30 40 measure
    COALESCE(pcent_30_40,0) AS abs_30_40,
    COALESCE(pct_live_work_local_area,0) AS abs_live_sa1_work_sa3
FROM parcel_dwellings p
LEFT JOIN area_linkage a ON p.mb_code_20 = a.mb_code_2016
LEFT JOIN (SELECT DISTINCT({id}) FROM excluded_parcels) e ON p.{id} = e.{id}
LEFT JOIN nh_inds_distance ON p.{id} = nh_inds_distance.{id}
LEFT JOIN sc_nh1600m ON p.{id} = sc_nh1600m.{id}
LEFT JOIN dd_nh1600m ON p.{id} = dd_nh1600m.{id}
LEFT JOIN ind_os_distance ON p.{id} = ind_os_distance.{id}
LEFT JOIN abs_ind_30_40 h ON a.sa1_7digitcode_2016 = h.sa1_7digitcode_2016::text
LEFT JOIN live_sa1_work_sa3 l ON a.sa1_7digitcode_2016 = l.sa1_7digitcode_2016::text
LEFT JOIN d_3200m_cl."fruit_veg_osm" ON p.{id} = d_3200m_cl."fruit_veg_osm".{id}
LEFT JOIN d_3200m_cl."meat_seafood_osm" ON p.{id} = d_3200m_cl."meat_seafood_osm".{id}
LEFT JOIN d_3200m_cl."supermarket_osm" ON p.{id} = d_3200m_cl."supermarket_osm".{id}
LEFT JOIN d_3200m_cl."convenience_osm" ON p.{id} = d_3200m_cl."convenience_osm".{id}
LEFT JOIN d_3200m_cl."newsagent_osm" ON p.{id} = d_3200m_cl."newsagent_osm".{id}
LEFT JOIN d_3200m_cl."petrolstation_osm" ON p.{id} = d_3200m_cl."petrolstation_osm".{id}
LEFT JOIN d_3200m_cl."museum_osm" ON p.{id} = d_3200m_cl."museum_osm".{id}
LEFT JOIN d_3200m_cl."art_gallery_osm" ON p.{id} = d_3200m_cl."art_gallery_osm".{id}
LEFT JOIN d_3200m_cl."cinema_osm" ON p.{id} = d_3200m_cl."cinema_osm".{id}
LEFT JOIN d_3200m_cl."theatre_osm" ON p.{id} = d_3200m_cl."theatre_osm".{id}
LEFT JOIN d_3200m_cl."libraries" ON p.{id} = d_3200m_cl."libraries".{id}
LEFT JOIN d_3200m_cl."childcare_oshc_meet" ON p.{id} = d_3200m_cl."childcare_oshc_meet".{id}
LEFT JOIN d_3200m_cl."childcare_all_meet" ON p.{id} = d_3200m_cl."childcare_all_meet".{id}
LEFT JOIN d_3200m_cl."nhsd_2017_aged_care_residential" ON p.{id} = d_3200m_cl."nhsd_2017_aged_care_residential".{id}
LEFT JOIN d_3200m_cl."nhsd_2017_pharmacy" ON p.{id} = d_3200m_cl."nhsd_2017_pharmacy".{id}
LEFT JOIN d_3200m_cl."nhsd_2017_mc_family_health" ON p.{id} = d_3200m_cl."nhsd_2017_mc_family_health".{id}
LEFT JOIN d_3200m_cl."nhsd_2017_other_community_health_care" ON p.{id} = d_3200m_cl."nhsd_2017_other_community_health_care".{id}
LEFT JOIN d_3200m_cl."nhsd_2017_dentist" ON p.{id} = d_3200m_cl."nhsd_2017_dentist".{id}
LEFT JOIN d_3200m_cl."nhsd_2017_gp" ON p.{id} = d_3200m_cl."nhsd_2017_gp".{id}
LEFT JOIN d_3200m_cl."public_swimming_pool_osm" ON p.{id} = d_3200m_cl."public_swimming_pool_osm".{id}
WHERE e.{id} IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ix_uli_inds ON  uli_inds ({id});
'''.format(id = points_id)
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

print('\r\nCreate address level score card...'),
sql = '''
DROP TABLE IF EXISTS ind_score_card;
CREATE TABLE IF NOT EXISTS ind_score_card AS
SELECT 
    p.{id}                    ,
    p.count_objectid          ,
    p.point_x                 ,
    p.point_y                 ,
    p.hex_id                  ,
    '{full_locale}'::text AS study_region,
    '{locale}'::text AS locale      ,
    area.mb_code_2016         ,
    area.mb_category_name_2016,
    area.sa1_maincode_2016    ,
    area.sa2_name_2016        ,
    area.sa3_name_2016        ,
    area.sa4_name_2016        ,
    area.gccsa_name_2016      ,
    area.state_name_2016      ,
    area.ssc_name_2016        ,
    area.lga_name_2016        ,
    area.ucl_name_2016        ,
    area.sos_name_2016        ,
    area.urban                ,
    area.irsd_score           ,
    e.exclude                 ,
    dl_soft_1600m AS daily_living,
    sc_nh1600m AS street_connectivity,
    dd_nh1600m AS dwelling_density,
    ind_walkability.walkability_index,
    uli.uli AS liveability_index,
    array_min(activity_centres.distances) AS closest_activity_centre,
    array_min(alcohol_offlicence.distances) AS closest_alcohol_offlicence,
    ind_si_mix.si_mix AS social_infrastructure_mix,
    threshold_hard(nh_inds_distance.gtfs_20191008_20191205_frequent_pt_0030,400) AS frequent_pt_400m,
    threshold_hard(ind_os_distance.pos_15k_sqm_distance_m,400) AS large_pos_400m,
    live_sa1_work_sa3.pct_live_work_local_area ,
    abs_ind_30_40.pcent_30_40 as pct_30_40_affordable_housing,
    p.geom              
FROM     
parcel_dwellings p                                                                                 
LEFT JOIN area_linkage area ON p.mb_code_20 = area.mb_code_2016
LEFT JOIN (SELECT {id}, string_agg(indicator,',') AS exclude 
           FROM excluded_parcels GROUP BY {id}) e 
       ON p.{id} = e.{id}
LEFT JOIN ind_daily_living ON p.{id} = ind_daily_living.{id}
LEFT JOIN sc_nh1600m ON p.{id} = sc_nh1600m.{id}
LEFT JOIN dd_nh1600m ON p.{id} = dd_nh1600m.{id}
LEFT JOIN uli ON p.{id} = uli.{id}
LEFT JOIN ind_walkability ON p.{id} = ind_walkability.{id}
LEFT JOIN d_3200m_cl.activity_centres ON p.{id} = d_3200m_cl.activity_centres.{id}
LEFT JOIN d_3200m_cl.alcohol_offlicence ON p.{id} = d_3200m_cl.alcohol_offlicence.{id}
LEFT JOIN ind_si_mix ON p.{id} = ind_si_mix.{id}
LEFT JOIN nh_inds_distance ON p.{id} = nh_inds_distance.{id}
LEFT JOIN ind_os_distance ON p.{id} = ind_os_distance.{id}
LEFT JOIN live_sa1_work_sa3 ON area.sa1_7digitcode_2016 = live_sa1_work_sa3.sa1_7digitcode_2016::text
LEFT JOIN abs_ind_30_40 ON area.sa1_7digitcode_2016 = abs_ind_30_40.sa1_7digitcode_2016::text;
CREATE UNIQUE INDEX IF NOT EXISTS ix_score_card ON  ind_score_card ({id});
CREATE INDEX IF NOT EXISTS gix_score_card ON ind_score_card USING GIST (geom);
'''.format(id = points_id, 
           full_locale = full_locale,
           locale = locale)
curs.execute(sql)
conn.commit()
print('Done.')

print('\r\nCreate area level score cards...')
print('  - Mesh Block')
sql = '''
DROP TABLE IF EXISTS ind_score_card_mb_init;
CREATE TABLE IF NOT EXISTS ind_score_card_mb_init AS
SELECT a.mb_code_2016          ,
       a.mb_category_name_2016 ,
       t.study_region,
       t.locale,
       a.dwelling              ,
       a.person                ,
       a.sa1_maincode_2016     ,
       a.sa2_name_2016         ,
       a.sa3_name_2016         ,
       a.sa4_name_2016         ,
       a.gccsa_name_2016       ,
       a.state_name_2016       ,
       a.ssc_name_2016         ,
       a.lga_name_2016         ,
       a.ucl_name_2016         ,
       a.sos_name_2016         ,
       a.urban                 ,
       a.irsd_score            ,
       a.area_ha               ,
        daily_living,
        street_connectivity,
        dwelling_density,
        walkability_index,
        liveability_index,
        social_infrastructure_mix,
        closest_activity_centre,
        closest_alcohol_offlicence,
        frequent_pt_400m,
        large_pos_400m,
        pct_live_work_local_area, 
        pct_30_40_affordable_housing,
       sample_count                                   ,
       sample_count / a.area_ha AS sample_count_per_ha,
       a.geom                 
FROM area_linkage a 
LEFT JOIN (
    SELECT  p.mb_code_2016,
            string_agg(DISTINCT(p.study_region),',')::varchar study_region,
            string_agg(DISTINCT(p.locale),',')::varchar locale,
            COUNT(p.*) AS sample_count       ,
            AVG(daily_living) AS daily_living,
            AVG(street_connectivity) AS street_connectivity,
            AVG(walkability_index) AS walkability_index,
            AVG(dwelling_density) AS dwelling_density,
            AVG(liveability_index) AS liveability_index,
            AVG(closest_activity_centre) AS closest_activity_centre,
            AVG(closest_alcohol_offlicence) AS closest_alcohol_offlicence,
            AVG(social_infrastructure_mix)  AS social_infrastructure_mix,
            100 * AVG(frequent_pt_400m) AS frequent_pt_400m,
            100 * AVG(large_pos_400m) AS large_pos_400m,
            AVG(pct_live_work_local_area) pct_live_work_local_area ,
            AVG(pct_30_40_affordable_housing) as pct_30_40_affordable_housing
    FROM ind_score_card p
    WHERE p.exclude IS NULL
    GROUP BY p.mb_code_2016) t USING (mb_code_2016)
WHERE a.irsd_score IS NOT NULL
  AND a.dwelling > 0
  AND a.urban = 'urban'
  AND a.study_region IS TRUE
  AND sample_count > 0
;
CREATE UNIQUE INDEX IF NOT EXISTS ix_area_indicators_mb_json ON  area_indicators_mb_json (mb_code_2016);
CREATE INDEX IF NOT EXISTS gix_area_indicators_mb_json ON area_indicators_mb_json USING GIST (geom);
'''
curs.execute(sql)
conn.commit()
print('Done.')

ind_list = ['daily_living',
        'street_connectivity',
        'dwelling_density',
        'walkability_index',
        'liveability_index',
        'social_infrastructure_mix',
        'closest_activity_centre',
        'closest_alcohol_offlicence',
        'frequent_pt_400m',
        'large_pos_400m',
        'pct_live_work_local_area', 
        'pct_30_40_affordable_housing']

print("Creating weighted area aggregate tables:")
for area in analysis_regions + ['study region']:   
    if area != 'study region':
        area_id = df_regions.loc[area,'id']
        abbrev = df_regions.loc[area,'abbreviation']
        include_region = 'study_region,'
    else: 
        area_id = 'study_region'
        abbrev  = 'region'
        include_region = ''
    if area != 'Section of State':
        pkey = area_id
    else: 
        pkey = '{},study_region'.format(area_id)
    for standard in ['dwelling','person']:
        print("  - score_card_{}_{}".format(abbrev,standard))
        sql = '''
        DROP TABLE IF EXISTS score_card_{abbrev}_{standard};
        CREATE TABLE score_card_{abbrev}_{standard} AS
        SELECT 
        {area_code},
        {include_region}
        locale,
        SUM(dwelling) AS dwelling,
        SUM(person) AS person,
        SUM(sample_count) AS sample_count,
        SUM(sample_count)/SUM(area_ha) AS sample_count_per_ha,
        SUM(area_ha) AS area_ha,
        {extract},
        ST_Union(geom) AS geom
        FROM ind_score_card_mb_init
        GROUP BY {area_code},study_region,locale;
        '''.format(area_code = area_id,
                   abbrev = abbrev,
                   include_region = include_region,
                   extract = ','.join(['''
                       (CASE             
                            -- if there are no units (dwellings or persons) the indicator is null
                            WHEN COALESCE(SUM({standard}),0) = 0
                                THEN NULL
                            -- else, calculate the value of the unit weighted indicator
                            ELSE                             
                               ((SUM({standard}*{i})::numeric)/SUM({standard}))::numeric
                          END) AS "{i}"
                   '''.format(i = i,standard = standard) for i in ind_list]),
                   standard = standard
                   )
        curs.execute(sql)
        conn.commit()
        sql = '''
        ALTER TABLE  score_card_{abbrev}_{standard} ADD PRIMARY KEY ({pkey});
        '''.format(pkey = pkey,
                   abbrev = abbrev,
                   standard = standard)
        curs.execute(sql)
        conn.commit()


# print("Done!")
# # output to completion log    
# script_running_log(script, task, start)
