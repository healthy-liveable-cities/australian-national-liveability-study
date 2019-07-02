# Script:  19_area_indicators.py
# Purpose: Create area level indicator tables
# Author:  Carl Higgs 
# Date:    20 July 2018

import os
import sys
import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create area level indicator tables for {}'.format(locale)

# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

# Indicator configuration sheet is 'df_inds', read in from config file in the config script

# Restrict to indicators associated with study region (except distance to closest dest indicators)
# the following two tables (indicators/measures, and distances to closest measures) will later be
# appended once the first table is expanded into soft and hard threshold indicator forms
ind_matrix = df_inds[df_inds['locale'].str.contains('|'.join([locale,'\*']))].copy()
ind_destinations = df_destinations[(df_destinations.locale == "*") | (df_destinations.locale == locale)].copy()
ind_destinations = ind_destinations.set_index('destination')
ind_destinations.index.name = 'indicators'
ind_destinations = ind_destinations.loc[:,'unit_level_description':]

# Get a list of destinations processed within this region for distance to closest
# sql = '''SELECT DISTINCT(dest_name) dest_name FROM od_closest ORDER BY dest_name;'''
sql = '''SELECT dest_name FROM dest_type ORDER BY dest_name;'''
curs.execute(sql)
categories = [x[0] for x in curs.fetchall()]

# # get the set of distance to closest regions which match for this region
# destinations = df_inds[df_inds['ind'].str.contains('destinations')]
# current_categories = [x for x in categories if 'distance_m_{}'.format(x) in destinations.ind_plain.str.encode('utf8').tolist()]
# ind_matrix = ind_matrix.append(destinations[destinations['ind_plain'].str.replace('distance_m_','').str.contains('|'.join(current_categories))])
ind_matrix['order'] = list(ind_matrix.index)
ind_soft = ind_matrix.loc[ind_matrix.tags=='_{threshold}',:].copy()
ind_hard = ind_matrix.loc[ind_matrix.tags=='_{threshold}',:].copy()
ind_soft.replace(to_replace='{threshold}', value='soft', inplace=True,regex=True)
ind_hard.replace(to_replace='{threshold}', value='hard', inplace=True,regex=True)

ind_matrix = pandas.concat([ind_matrix,ind_soft,ind_hard], ignore_index=True).sort_values('ind')
ind_matrix.drop(ind_matrix[ind_matrix.tags == '_{threshold}'].index, inplace=True)
# Restrict to indicators with a defined query
ind_matrix = ind_matrix[pandas.notnull(ind_matrix['Query'])]
ind_matrix.drop(ind_matrix[ind_matrix['updated?'] == 'n'].index, inplace=True)

# Make concatenated indicator and tag name (e.g. 'walk_14' + 'hard')
# Tags could be useful later as can allow to search by name for e.g. threshold type,
# or other keywords (policy, binary, obsolete, planned --- i don't know, whatever)
# These tags are tacked on the end of the ind name seperated with underscores
ind_matrix['indicators'] = ind_matrix['ind'] + ind_matrix['tags'].fillna('')
# ind_matrix['sort_cat'] = pandas.Categorical(ind_matrix['ind'], categories=mylist, ordered=True)
# ind_matrix.sort_values('sort_cat', inplace=True)
# Compile list of indicators
ind_matrix.sort_values('order', inplace=True)

# Create an indicators summary table
ind_matrix = ind_matrix.set_index('indicators')
ind_matrix = ind_matrix.append(ind_destinations)
ind_list = ind_matrix.index.values

indicator_tuples =  list(zip(ind_matrix.index,ind_matrix.agg_scale,ind_matrix.aggregate_description))
print("Creating Mesh Block level indicator table... "),
sql = '''
DROP TABLE IF EXISTS area_indicators_mb_json;
CREATE TABLE area_indicators_mb_json AS
SELECT a.mb_code_2016          ,
       a.mb_category_name_2016 ,
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
       jsonb_agg(
          to_jsonb(
              (SELECT i FROM
                  (SELECT
                      {indicators}
                  ) i))) AS indicators                ,
       sample_count                                   ,
       sample_count / a.area_ha AS sample_count_per_ha,
       a.geom                 
FROM area_linkage a 
LEFT JOIN (
    SELECT p.mb_code_2016,
           COUNT(p.*) AS sample_count       ,
          {jsonb_inds}
    FROM parcel_indicators p
    LEFT JOIN dest_closest_indicators USING(gnaf_pid)
    WHERE p.exclude IS NULL
    GROUP BY p.mb_code_2016) t USING (mb_code_2016)
WHERE a.irsd_score IS NOT NULL
  AND a.dwelling > 0
  AND a.urban = 'urban'
  AND a.study_region IS TRUE
GROUP BY a.mb_code_2016,
         a.mb_category_name_2016 ,
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
         sample_count            ,
         sample_count_per_ha     ,
         a.geom
;
CREATE UNIQUE INDEX IF NOT EXISTS ix_area_indicators_mb_json ON  area_indicators_mb_json (mb_code_2016);
CREATE INDEX IF NOT EXISTS gix_area_indicators_mb_json ON area_indicators_mb_json USING GIST (geom);
'''.format(indicators = '"{}"'.format('","'.join(ind_list)),
           jsonb_inds = jsonb_summary_sql(indicator_tuples))
# print(sql)
curs.execute(sql)
conn.commit()
print("Done.")
# The above can be used like so:
## SELECT mb_code_2016,sample_count,sample_count_per_ha,jsonb_pretty(indicators) AS indicators FROM abs_indicators LIMIT 1;
## 
## SELECT mb_code_2016, 
##        inds
## FROM abs_indicators, 
##     jsonb_array_elements(indicators) inds
## LIMIT 10;
## 
## SELECT mb_code_2016,
##        (ind->'walk_16')::jsonb->'mean' AS walk_16
## FROM abs_indicators,jsonb_array_elements(indicators) ind LIMIT 1;

## sql = '''
## SELECT mb_code_2016,
##        {extract}
## FROM abs_indicators,jsonb_array_elements(indicators) ind LIMIT 1;
## '''.format(extract = ','.join(["(ind->'{i}')->'mean' AS {i}".format(i = i) for i in ind_list]))


print("Creating weighted area aggregate tables:")
for area in analysis_regions:  
    area_id = df_regions.loc[area,'id']
    abbrev = df_regions.loc[area,'abbreviation']
    for standard in ['dwelling','person']:
        print("  - {}_ind_{}".format(abbrev,standard))
        sql = '''
        DROP TABLE IF EXISTS {abbrev}_ind_{standard};
        CREATE TABLE {abbrev}_ind_{standard} AS
        SELECT 
        {area_code},
        SUM(dwelling) AS dwelling,
        SUM(person) AS person,
        SUM(sample_count) AS sample_count,
        SUM(sample_count)/SUM(area_ha) AS sample_count_per_ha,
        SUM(area_ha) AS area_ha,
        {extract},
        ST_Union(geom) AS geom
        FROM area_indicators_mb,
             jsonb_array_elements(indicators) ind
        GROUP BY {area_code};
        ALTER TABLE  {abbrev}_ind_{standard} ADD PRIMARY KEY ({area_code});
        '''.format(area_code = area_id,
                   abbrev = abbrev,
                   extract = ','.join(['''
                       (CASE                                                       
                            WHEN COALESCE(SUM({standard}),0) = 0
                                THEN NULL
                            ELSE                             
                                ROUND(
                                  (SUM({standard}*((ind->'{i}')->>'mean')::numeric)/SUM({standard}))::numeric,
                                  {rounding}
                                  ) 
                          END) AS "{i}"
                   '''.format(i = i,rounding=1,standard = standard) for i in ind_list]),
                   standard = standard
                   )
        curs.execute(sql)
        conn.commit()
# create study region result
for standard in ['dwelling','person']:
    print("  - {}_ind_{}".format('region',standard))
    sql = '''
    DROP TABLE IF EXISTS {abbrev}_ind_{standard};
    CREATE TABLE {abbrev}_ind_{standard} AS
    SELECT 
    '{area_code}' AS region,
    SUM(dwelling) AS dwelling,
    SUM(person) AS person,
    SUM(sample_count) AS sample_count,
    SUM(sample_count)/SUM(area_ha) AS sample_count_per_ha,
    SUM(area_ha) AS area_ha,
    {extract},
    ST_Union(geom) AS geom
    FROM abs_indicators,
         jsonb_array_elements(indicators) ind
    GROUP BY region;
    ALTER TABLE  {abbrev}_ind_{standard} ADD PRIMARY KEY (region);
    '''.format(area_code = full_locale,
               abbrev = 'region',
               extract = ','.join(['''
                   (CASE                                                       
                        WHEN COALESCE(SUM({standard}),0) = 0
                            THEN NULL                                          
                        ELSE                                      
                            ROUND(
                              (SUM({standard}*((ind->'{i}')->>'mean')::numeric)/SUM({standard}))::numeric,
                              {rounding}
                              ) 
                      END) AS "{i}"
               '''.format(i = i,rounding=1,standard = standard) for i in ind_list]),
               standard = standard
               )
    curs.execute(sql)
    conn.commit()
print("Done.")

# for area in analysis_regions + ['region']:  
  # print("    - aggregate indicator table li_inds_{}... ".format(area)),
  # createTable = '''
  # DROP TABLE IF EXISTS li_inds_{area} ; 
  # CREATE TABLE li_inds_{area} AS
  # SELECT {area_code2},
    # COUNT(*) AS sample_point_count,
    # {indicators}
    # FROM parcel_indicators p
    # LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    # {exclusion}
    # GROUP BY {area_code}
    # ORDER BY {area_code} ASC;
  # ALTER TABLE li_inds_{area} ADD PRIMARY KEY ({area_code3});
  # '''.format(area = area,
             # area_code = area_code,
             # area_code2 = area_code2,
             # area_code3 = area_code3,
             # indicators = ind_avg,
             # exclusion = exclusion_criteria)
  # curs.execute(createTable)
  # conn.commit()
  # print("Done.")
  
  
  # ### Note: for now, we are just doing the continuous scale indicators with averages; later I'll implement a second pass to evaluate the threshold cutoffs.
  
  # print("    - sd summary table li_sd_{}... ".format(area)),
  # createTable = '''
  # DROP TABLE IF EXISTS li_sd_{area} ; 
  # CREATE TABLE li_sd_{area} AS
  # SELECT {area_code2},
    # {indicators}     
    # FROM parcel_indicators p
    # LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    # {exclusion}
    # GROUP BY {area_code}
    # ORDER BY {area_code} ASC;
  # ALTER TABLE li_sd_{area} ADD PRIMARY KEY ({area_code3});
  # '''.format(area = area,
             # area_code = area_code,
             # area_code2 = area_code2,
             # area_code3 = area_code3,
             # indicators = ind_sd,
             # exclusion = exclusion_criteria)
  # curs.execute(createTable)
  # conn.commit()
  # print("Done.")
  
  # print("    - range summary table li_range_{}... ".format(area)),
  # createTable = '''
  # DROP TABLE IF EXISTS li_range_{area} ; 
  # CREATE TABLE li_range_{area} AS
  # SELECT {area_code2},
    # {indicators}     
    # FROM parcel_indicators p
    # LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    # {exclusion}
    # GROUP BY {area_code}
    # ORDER BY {area_code} ASC;
  # ALTER TABLE li_range_{area} ADD PRIMARY KEY ({area_code3});
  # '''.format(area = area,
             # area_code = area_code,
             # area_code2 = area_code2,
             # area_code3 = area_code3,
             # indicators = ind_range,
             # exclusion = exclusion_criteria)
  # curs.execute(createTable)
  # conn.commit()
  # print("Done.")
  
  # print("    - median summary table li_median_{}... ".format(area)),  
  # createTable = '''
  # DROP TABLE IF EXISTS li_median_{area} ; 
  # CREATE TABLE li_median_{area} AS
  # SELECT {area_code2},
    # {indicators}     
    # FROM parcel_indicators p
    # LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    # {exclusion}
    # GROUP BY {area_code}
    # ORDER BY {area_code} ASC;
  # ALTER TABLE li_median_{area} ADD PRIMARY KEY ({area_code3});
  # '''.format(area = area,
             # area_code = area_code,
             # area_code2 = area_code2,
             # area_code3 = area_code3,
             # indicators = ind_median,
             # exclusion = exclusion_criteria)
  # curs.execute(createTable)
  # conn.commit()
  # print("Done.")
  
  # print("    - IQR summary table li_iqr_{}... ".format(area)),  
  # createTable = '''
  # DROP TABLE IF EXISTS li_iqr_{area} ; 
  # CREATE TABLE li_iqr_{area} AS
  # SELECT {area_code2},
    # {indicators}     
    # FROM parcel_indicators p
    # LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    # {exclusion}
    # GROUP BY {area_code}
    # ORDER BY {area_code} ASC;
  # ALTER TABLE li_iqr_{area} ADD PRIMARY KEY ({area_code3});
  # '''.format(area = area,
             # area_code = area_code,
             # area_code2 = area_code2,
             # area_code3 = area_code3,
             # indicators = ind_iqr,
             # exclusion = exclusion_criteria)
  # curs.execute(createTable)
  # conn.commit()
  # print("Done.")
  
  # map_ind_percentile_area = ''
  # if area != 'region':
    # print("    - percentile summary table li_percentiles_{}... ".format(area)),      
    # createTable = '''
    # DROP TABLE IF EXISTS li_percentiles_{area} ; 
    # CREATE TABLE li_percentiles_{area} AS
    # SELECT {area_code4},
           # {indicators}
    # FROM li_inds_{area}
    # ORDER BY {area_code} ASC;
  # ALTER TABLE li_percentiles_{area} ADD PRIMARY KEY ({area_code});
  # '''.format(area = area,
             # area_code = area_code3,
             # area_code4 = area_code4,
             # indicators = ind_percentile)
    # curs.execute(createTable)
    # conn.commit()
    # print("Done.")
    # map_ind_percentile_area = '\n{},'.format(map_ind_percentile)
    
  # if area_code3 != 'mb_code_2016':
    # # Create shape files for interactive map visualisation
    # area_strings   = {'sa1_maincode' :'''area.sa1_maincode AS sa1   ,\n area.suburb ,\n area.lga ,area.resid_parcels, area.dwellings, area.resid_persons \n''',
                      # 'ssc_name_2016':''' '-'::varchar AS sa1 ,\n area.suburb AS suburb,\n area.lga AS lga , area.resid_parcels, area.dwellings, area.resid_persons \n''',
                      # 'lga_name_2016':''' '-'::varchar AS sa1 ,\n '-'::varchar AS suburb ,\n area.lga  AS lga ,area.resid_parcels, area.dwellings, area.resid_persons \n''',
                      # 'sos_name_2016':''' '-'::varchar AS sa1 ,\n '-'::varchar AS suburb ,\n '-'  AS lga , area.sos_name_2016 AS sos \n''',
                      # 'region':''' 'region':: varchar AS region '''
                      # }    
              
    # area_code2 = {'sa1_maincode' :'sa1_maincode',
                  # 'ssc_name_2016':'suburb',
                  # 'lga_name_2016':'lga',
                  # 'sos_name_2016':'sos_name_2016',
                  # 'region':'region'}    

    # area_names2 = {'sa1_maincode' :'sa1_mainco',
                  # 'ssc_name_2016':'ssc_name_2',
                  # 'lga_name_2016':'lga_name_2',
                  # 'sos_name_2016':'sos_name_2016',
                  # 'region':'region'}                      

    # area_tables = {'sa1_maincode' :'area_sa1',
                  # 'ssc_name_2016':'area_ssc',
                  # 'lga_name_2016':'area_lga',
                  # 'sos_name_2016': 'study_region_urban',
                  # 'region': ''' (SELECT 'region'::varchar AS region, ST_Union(geom) AS geom FROM study_region_urban) '''}   
                  
    # area_code_tables = {'sa1_maincode' :'''LEFT JOIN main_sa1_2016_aust_full AS area_code ON area.sa1_maincode = area_code.sa1_mainco''',
                        # 'ssc_name_2016':'''LEFT JOIN main_ssc_2016_aust      AS area_code ON area.suburb       = area_code.ssc_name_2''',
                        # 'lga_name_2016':'''LEFT JOIN main_lga_2016_aust      AS area_code ON area.lga          = area_code.lga_name_2''',
                        # 'sos_name_2016': ''' ''',
                        # 'region': ''' '''}
                      
    # community_code = {'sa1_maincode' :'''area_code.sa1_7digit AS community_code''',
                      # 'ssc_name_2016':'''CONCAT('SSC',area_code.ssc_code_2::varchar) AS community_code''',
                      # 'lga_name_2016':'''CONCAT('LGA',area_code.lga_code_2::varchar) AS community_code''',
                      # 'sos_name_2016':''' '-'::varchar ''',
                      # 'region':''' '-'::varchar '''}

    # boundary_tables = {'sa1_maincode' :'''main_sa1_2016_aust_full b WHERE b.sa1_mainco IN (SELECT sa1_maincode FROM area_sa1) ''',
                       # 'ssc_name_2016': '''main_ssc_2016_aust b WHERE b.ssc_name_2 IN (SELECT suburb FROM area_ssc) ''',
                       # 'lga_name_2016': '''main_lga_2016_aust b WHERE b.lga_name_2 IN (SELECT lga FROM area_lga) ''',
                       # 'sos_name_2016': '''study_region_urban b ''',
                       # 'region': ''' (SELECT 'region'::varchar AS region, ST_Union(geom) AS geom FROM study_region_urban) b ''',}
                       
    # percentile_join_string = ' '              
    # if area != 'region':
      # percentile_join_string = '''
      # LEFT JOIN li_percentiles_{area} AS perc 
             # ON area.{area_code2} = perc.{area_code}
      # '''.format(area = area,area_code = area_code3, area_code2 = area_code2[area_code3])

    # # Note -i've excerpted SD and median out of the below table for now; too much for SA1s  
    # #        {sd},
    # #        {median},
    # # LEFT JOIN li_median_{area} AS median ON area.{area_code2} = median.{area_code}
    # # LEFT JOIN li_sd_{area} AS sd ON area.{area_code2} = raw.{area_code}
    # createTable = '''DROP TABLE IF EXISTS li_map_{area};
    # CREATE TABLE li_map_{area} AS
    # SELECT {area_strings},
           # {raw},
           # {percentile}
           # {range},
           # {iqr},
           # {community_code},
           # ST_TRANSFORM(area.geom,4326) AS geom              
    # FROM {area_table} AS area
    # LEFT JOIN li_inds_{area} AS raw ON area.{area_code2} = raw.{area_code}
    # {percentile_join}
    # LEFT JOIN li_range_{area} AS range ON area.{area_code2} = range.{area_code}
    # LEFT JOIN li_iqr_{area} AS iqr ON area.{area_code2} = iqr.{area_code}
    # {area_code_table};
    # '''.format(area = area,
               # area_code = area_code3,
               # area_table = area_tables[area_code3],
               # area_strings = area_strings[area_code3],
               # raw = map_ind_raw,
               # sd = map_ind_sd,
               # percentile = map_ind_percentile_area,
               # range = map_ind_range,
               # median = map_ind_median,
               # iqr = map_ind_iqr,
               # community_code = community_code[area_code3],
               # area_code2 = area_code2[area_code3],
               # area_code_table = area_code_tables[area_code3],
               # percentile_join = percentile_join_string)
    # print("    - Create table for mapping indicators at {} level".format(area))
    # curs.execute(createTable)
    # conn.commit()
    
    # createTable = '''
    # DROP TABLE IF EXISTS boundaries_{area};
    # CREATE TABLE boundaries_{area} AS
    # SELECT b.{area_names2} AS {area_code},
            # ST_Transform(b.geom,4326) AS geom         
    # FROM {boundaries};
    # '''.format(area = area,
               # area_names2 = area_names2[area_code3],
               # area_code = area_code3,
               # boundaries = boundary_tables[area_code3])
    # print("    - boundary overlays at {} level".format(area)),
    # curs.execute(createTable)
    # conn.commit()
    # print("Done.")
    
# print("Output to geopackage gpkg: {path}/li_map_{db}.gpkg... ".format(path = map_features_outpath, db = db)),
# # need to add in a geometry column to ind_description to allow for importing of this table as a layer in geoserver
# # If it doesn't already exists
# # So, check if it already exists
# curs.execute("SELECT column_name FROM information_schema.columns WHERE table_name='ind_description' and column_name='geom';")
# null_geom_check = curs.fetchall()
# if len(null_geom_check)==0:
  # # if geom doesn't exist, created it
  # curs.execute("SELECT AddGeometryColumn ('public','ind_description','geom',4326,'POINT',2);")
  # conn.commit()
# # Output to geopackage using ogr2ogr; note that this command is finnicky and success depends on version of ogr2ogr that you have  
# command = 'ogr2ogr -overwrite -f GPKG {path}/li_map_{db}.gpkg PG:"host={host} user={user} dbname={db} password={pwd}" '.format(path = map_features_outpath,
                                                                                                                               # host = db_host,
                                                                                                                               # user = db_user,
                                                                                                                               # pwd = db_pwd,
                                                                                                                               # db = db) \
          # + ' "li_map_sa1" "li_map_ssc" "li_map_lga" "li_map_sos" "li_map_region" "ind_description" "boundaries_sa1" "boundaries_ssc" "boundaries_lga" "study_region_urban"  "study_region_not_urban" "area_no_dwelling" "area_no_irsd"'
# sp.call(command)
# print("Done.")


# print("Can you please also run the following from the command prompt in the following directory: {folderPath}/study_region//wgs84_epsg4326/".format(folderPath = folderPath))
# print('pg_dump -U postgres -h localhost -W  -t "li_map_sa1" -t "li_map_ssc" -t "li_map_lga" -t "li_map_sos" -t "li_map_region" -t "ind_description" -t "boundaries_sa1" -t "boundaries_ssc" -t "boundaries_lga" -t "study_region_urban"  -t "study_region_not_urban" -t "area_no_dwelling" -t "area_no_irsd" {db} > {db}.sql'.format(db = db))

# print("Created SA1, suburb and LGA level tables for map web app.")
  
# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
