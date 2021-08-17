import os
import sys
import time
import datetime
import psycopg2 
import numpy as np
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger
from psycopg2.extensions import register_adapter, AsIs
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create additional highlife indicator tables for {}'.format(locale)

# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db), 
                       use_native_hstore=False)

# Travel time to CBD
# import distance to CBD custom data 

# The following columns were mapped to:
# 'Building ID'
# 'Street No. Postal'
# 'Street Name Postal'
# 'Street Type Postal '
# 'Suburb'
# 'State'
# 'Postcode'
# 'Travel time to CBD by car (mins)'
# 'Travel time to CBD by public transport (mins)'
# '% difference in travel time to CBD between public transport and car'

locale_dict = {'perth':'WA','melbourne':'VIC','sydney':'NSW'}

new_columns = ['building_id',
              'street_no_postal',
              'street_name_postal',
              'street_type_postal',
              'suburb',
              'state',
              'postcode',
              'travel_time_to_cbd_by_car_mins',
              'travel_time_to_cbd_by_public_transport_mins',
              'pct_difference_in_travel_time_to_cbd_between_pt_and_car']

xls = pandas.ExcelFile('../data/destinations/Inputs_for_transport_indicators_for_High_Life_study/High Life study - Variable 8 - Travel time to CBD by car & PT.xlsx')
df = pandas.read_excel(xls, "Variable 8")
df['% difference in travel time to CBD between public transport and car'] = 100*df['% difference in travel time to CBD between public transport and car']
# rename id to match that used in database
df.columns = new_columns
df.set_index('building_id',inplace=True)
df = df.query("state == '{}'".format(locale_dict[locale]))
df.to_sql('distance_to_cbd',engine,if_exists='replace')


df_uli = pandas.read_csv('../data/ntnl_li_sa1_dwelling_2018_20200319.csv')
df_uli.set_index('sa1_maincode_2016',inplace=True)
df_uli = df_uli.query("locale == '{}'".format(locale))
df_uli[['ntnl_city_2018_walkability','ntnl_city_2018_uli']].to_sql('ntnl_city_2018_walkability_uli',engine,if_exists='replace',schema='ind_sa1')

# NOTE: the distance to cbd data is currently not usable/matchable as the block id was not provided - so unable to match to specific polygons
# ie. the id 'building id' (corresponding to 'buildlingno' I assume) is not unique, and I don't have the address data for matching
                 
destinations = ['hl_food_fast',
                'hl_food_healthier',
                'hl_food_fresh',
                'hl_food_other_specialty',
                'fruit_veg_osm',
                'hl_food_specialty',
                'hl_dining_takeout',
                'hl_dining',
                'hl_convenience',
                'hl_alcohol_onlicence',
                'gambling_osm',
                'tobacco_osm',
                'supermarkets_2017',
                'alcohol_offlicence',
                'alcohol_onlicence']
distances = [100, 200, 400, 500, 800, 1000, 1600, 3200]

parcel_sql = []
parcel_sources = []
area_sql = []

for distance in distances:
    parcel_sql.append("ind_point.nh{distance}m.area_sqkm AS service_area_sqkm_{distance}m".format(distance=distance))
    parcel_sources.append("LEFT JOIN ind_point.nh{distance}m ON p.{points_id} = ind_point.nh{distance}m.{points_id}".format(distance=distance,
                                                                                                              points_id=points_id))
    area_sql.append("AVG(service_area_sqkm_{distance}m) AS service_area_sqkm_{distance}m".format(distance=distance))

for destination in destinations:
    for distance in distances:
        parcel_sql.append("count_in_threshold({destination}.distances,{distance}) AS {destination}_count_{distance}m".format(destination=destination,
                                                                                                                  distance=distance))
        area_sql.append("AVG({destination}_count_{distance}m) AS {destination}_count_{distance}m".format(destination=destination,
                                                                                                             distance=distance))
    parcel_sources.append("LEFT JOIN d_3200m_cl.{destination} ON p.{points_id} = d_3200m_cl.{destination}.{points_id}".format(destination=destination,
                                                                                                              points_id=points_id))
                                                                                                              
print("Creating access point indicators...")
sql = '''
   DROP TABLE IF EXISTS ind_point.parcel_hl_inds_nh;
   CREATE TABLE ind_point.parcel_hl_inds_nh AS
   SELECT p.{points_id}                                          ,
          p.buildlingno                                          ,
          p.buildname                                            ,
          p.blockid                                              , 
          p.blockname                                            ,
          p.wave                                                 ,
          '{full_locale}'::text AS study_region                  ,
          '{locale}'::text AS locale                             ,
          p.mb_code_2016                                         ,
          p.sa1_maincode_2016                                    ,
          p.sa2_name_2016                                        ,
          ind_sa1.ntnl_city_2018_walkability_uli."ntnl_city_2018_walkability"  AS sa1_ntnl_city_2018_walkability,
          ind_sa1.ntnl_city_2018_walkability_uli."ntnl_city_2018_uli"          AS sa1_ntnl_city_2018_uli,
          ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_score"  AS sa1_2011_irsd_aust_score  ,
          ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_rank"   AS sa1_2011_irsd_aust_rank   ,
          ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_decile" AS sa1_2011_irsd_aust_decile ,
          ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_pctile" AS sa1_2011_irsd_aust_pctile ,
          ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_score"  AS sa1_2016_irsd_aust_score  ,
          ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_rank"   AS sa1_2016_irsd_aust_rank   ,
          ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_decile" AS sa1_2016_irsd_aust_decile ,
          ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_pctile" AS sa1_2016_irsd_aust_pctile ,
          ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_score"  AS sa2_2011_irsd_aust_score  ,
          ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_rank"   AS sa2_2011_irsd_aust_rank   ,
          ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_decile" AS sa2_2011_irsd_aust_decile ,
          ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_pctile" AS sa2_2011_irsd_aust_pctile ,
          ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_score"  AS sa2_2016_irsd_aust_score  ,
          ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_rank"   AS sa2_2016_irsd_aust_rank   ,
          ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_decile" AS sa2_2016_irsd_aust_decile ,
          ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_pctile" AS sa2_2016_irsd_aust_pctile ,
          {indicators}
    FROM ind_point.parcel_indicators p   
    LEFT JOIN ind_sa1.ntnl_city_2018_walkability_uli ON p.sa1_maincode_2016 = ind_sa1.ntnl_city_2018_walkability_uli.sa1_maincode_2016::text
    LEFT JOIN ind_sa1.abs_irsd_2011_sa1 ON p.sa1_maincode_2016 = ind_sa1.abs_irsd_2011_sa1.sa1_maincode_2016 
    LEFT JOIN ind_sa1.abs_irsd_2016_sa1 ON p.sa1_maincode_2016 = ind_sa1.abs_irsd_2016_sa1.sa1_maincode_2016 
    LEFT JOIN ind_sa2.abs_irsd_2011_sa2 ON p.sa2_name_2016 = ind_sa2.abs_irsd_2011_sa2.sa2_name_2016
    LEFT JOIN ind_sa2.abs_irsd_2016_sa2 ON p.sa2_name_2016 = ind_sa2.abs_irsd_2016_sa2.sa2_name_2016
    {sources}
    ORDER BY p.{points_id};
'''.format(points_id = points_id, 
           indicators = ',\n'.join(parcel_sql), 
           sources = '\n'.join(parcel_sources), 
           full_locale = full_locale,
           locale = locale)
curs.execute(sql)
conn.commit()
    
   
print("Creating area indicator tables... ")
print("  - block...")
sql = '''
    DROP TABLE IF EXISTS area_hl_nh_inds_block;
    CREATE TABLE         area_hl_nh_inds_block AS
    SELECT p.blockid               ,
           p.blockname             ,
           p.wave                  ,
           p.study_region          ,
           p.locale                ,
           p.sa1_maincode_2016     ,
           p.sa2_name_2016         ,
           ind_sa1.ntnl_city_2018_walkability_uli."ntnl_city_2018_walkability"  AS sa1_ntnl_city_2018_walkability,
           ind_sa1.ntnl_city_2018_walkability_uli."ntnl_city_2018_uli"  AS sa1_ntnl_city_2018_uli,
           ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_score"  AS sa1_2011_irsd_aust_score  ,
           ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_rank"   AS sa1_2011_irsd_aust_rank   ,
           ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_decile" AS sa1_2011_irsd_aust_decile ,
           ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_pctile" AS sa1_2011_irsd_aust_pctile ,
           ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_score"  AS sa1_2016_irsd_aust_score  ,
           ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_rank"   AS sa1_2016_irsd_aust_rank   ,
           ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_decile" AS sa1_2016_irsd_aust_decile ,
           ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_pctile" AS sa1_2016_irsd_aust_pctile ,
           ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_score"  AS sa2_2011_irsd_aust_score  ,
           ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_rank"   AS sa2_2011_irsd_aust_rank   ,
           ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_decile" AS sa2_2011_irsd_aust_decile ,
           ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_pctile" AS sa2_2011_irsd_aust_pctile ,
           ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_score"  AS sa2_2016_irsd_aust_score  ,
           ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_rank"   AS sa2_2016_irsd_aust_rank   ,
           ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_decile" AS sa2_2016_irsd_aust_decile ,
           ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_pctile" AS sa2_2016_irsd_aust_pctile ,
           {indicators}
    FROM ind_point.parcel_hl_inds_nh p   
    LEFT JOIN ind_sa1.ntnl_city_2018_walkability_uli ON p.sa1_maincode_2016 = ind_sa1.ntnl_city_2018_walkability_uli.sa1_maincode_2016::text
    LEFT JOIN ind_sa1.abs_irsd_2011_sa1 ON p.sa1_maincode_2016 = ind_sa1.abs_irsd_2011_sa1.sa1_maincode_2016 
    LEFT JOIN ind_sa1.abs_irsd_2016_sa1 ON p.sa1_maincode_2016 = ind_sa1.abs_irsd_2016_sa1.sa1_maincode_2016 
    LEFT JOIN ind_sa2.abs_irsd_2011_sa2 ON p.sa2_name_2016 = ind_sa2.abs_irsd_2011_sa2.sa2_name_2016
    LEFT JOIN ind_sa2.abs_irsd_2016_sa2 ON p.sa2_name_2016 = ind_sa2.abs_irsd_2016_sa2.sa2_name_2016
    GROUP BY  p.blockid                ,
              p.blockname              ,
              p.wave                   ,
              p.study_region           ,
              p.locale                 ,
              p.sa1_maincode_2016      ,
              p.sa2_name_2016          ,
              ind_sa1.ntnl_city_2018_walkability_uli."ntnl_city_2018_walkability"  ,
              ind_sa1.ntnl_city_2018_walkability_uli."ntnl_city_2018_uli"  ,        ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_score"  ,
              ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_rank"   ,
              ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_decile" ,
              ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_pctile" ,
              ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_score"  ,
              ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_rank"   ,
              ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_decile" ,
              ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_pctile" ,
              ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_score"  ,
              ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_rank"   ,
              ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_decile" ,
              ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_pctile" ,
              ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_score"  ,
              ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_rank"   ,
              ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_decile" ,
              ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_pctile" 
    ORDER BY p.blockid,p.wave;
'''.format(points_id = points_id, 
           indicators = ',\n'.join(area_sql),  
           full_locale = full_locale,
           locale = locale)
curs.execute(sql)
conn.commit()
    
print("  - building...")
sql = '''
DROP TABLE IF EXISTS area_hl_nh_inds_building;
CREATE TABLE         area_hl_nh_inds_building AS
SELECT p.buildlingno           ,
       p.buildname             ,
       p.wave                  ,
       p.study_region          ,
       p.locale                ,
       p.sa1_maincode_2016     ,
       p.sa2_name_2016         ,
       ind_sa1.ntnl_city_2018_walkability_uli."ntnl_city_2018_walkability"  AS sa1_ntnl_city_2018_walkability,
       ind_sa1.ntnl_city_2018_walkability_uli."ntnl_city_2018_uli"  AS sa1_ntnl_city_2018_uli,
       ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_score"  AS sa1_2011_irsd_aust_score  ,
       ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_rank"   AS sa1_2011_irsd_aust_rank   ,
       ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_decile" AS sa1_2011_irsd_aust_decile ,
       ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_pctile" AS sa1_2011_irsd_aust_pctile ,
       ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_score"  AS sa1_2016_irsd_aust_score  ,
       ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_rank"   AS sa1_2016_irsd_aust_rank   ,
       ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_decile" AS sa1_2016_irsd_aust_decile ,
       ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_pctile" AS sa1_2016_irsd_aust_pctile ,
       ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_score"  AS sa2_2011_irsd_aust_score  ,
       ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_rank"   AS sa2_2011_irsd_aust_rank   ,
       ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_decile" AS sa2_2011_irsd_aust_decile ,
       ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_pctile" AS sa2_2011_irsd_aust_pctile ,
       ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_score"  AS sa2_2016_irsd_aust_score  ,
       ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_rank"   AS sa2_2016_irsd_aust_rank   ,
       ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_decile" AS sa2_2016_irsd_aust_decile ,
       ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_pctile" AS sa2_2016_irsd_aust_pctile ,
   {indicators}
   FROM
   ind_point.parcel_hl_inds_nh p   
   LEFT JOIN ind_sa1.ntnl_city_2018_walkability_uli ON p.sa1_maincode_2016 = ind_sa1.ntnl_city_2018_walkability_uli.sa1_maincode_2016::text
   LEFT JOIN ind_sa1.abs_irsd_2011_sa1 ON p.sa1_maincode_2016 = ind_sa1.abs_irsd_2011_sa1.sa1_maincode_2016 
   LEFT JOIN ind_sa1.abs_irsd_2016_sa1 ON p.sa1_maincode_2016 = ind_sa1.abs_irsd_2016_sa1.sa1_maincode_2016 
   LEFT JOIN ind_sa2.abs_irsd_2011_sa2 ON p.sa2_name_2016 = ind_sa2.abs_irsd_2011_sa2.sa2_name_2016
   LEFT JOIN ind_sa2.abs_irsd_2016_sa2 ON p.sa2_name_2016 = ind_sa2.abs_irsd_2016_sa2.sa2_name_2016
   GROUP BY  p.buildlingno           ,
             p.buildname             ,
             p.wave                  ,
             p.study_region          ,
             p.locale                ,
             p.sa1_maincode_2016     ,
             p.sa2_name_2016         ,
              ind_sa1.ntnl_city_2018_walkability_uli."ntnl_city_2018_walkability"  ,
              ind_sa1.ntnl_city_2018_walkability_uli."ntnl_city_2018_uli"  ,   
              ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_score"  ,
              ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_rank"   ,
              ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_decile" ,
              ind_sa1.abs_irsd_2011_sa1."2011_sa1_irsd_aust_pctile" ,
              ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_score"  ,
              ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_rank"   ,
              ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_decile" ,
              ind_sa1.abs_irsd_2016_sa1."2016_sa1_irsd_aust_pctile" ,
              ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_score"  ,
              ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_rank"   ,
              ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_decile" ,
              ind_sa2.abs_irsd_2011_sa2."2011_sa2_irsd_aust_pctile" ,
              ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_score"  ,
              ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_rank"   ,
              ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_decile" ,
              ind_sa2.abs_irsd_2016_sa2."2016_sa2_irsd_aust_pctile" 
   ORDER BY p.buildlingno,p.wave;
'''.format(points_id = points_id, 
           indicators = ',\n'.join(area_sql),  
           full_locale = full_locale,
           locale = locale)
curs.execute(sql)
conn.commit()

sql = '''
SELECT * FROM ind_point.parcel_hl_inds_nh;
'''.format(points_id=points_id)
df = pandas.read_sql_query(sql,con=engine)
summary_overall = df.describe(include='all').transpose()
old_cols = summary_overall.columns
summary_overall['summary_date'] = datetime.datetime.now().isoformat()
summary_overall.columns = [x.replace('%','_pct') for x in summary_overall.columns]
summary_overall = summary_overall.sort_index()
summary_overall.drop_duplicates().to_sql('ind_summary_hl_inds',engine, if_exists='replace')

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()