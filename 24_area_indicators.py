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
ind_destinations['destination'] = ind_destinations['destination'].apply(lambda x: "dist_m_{}".format(x))
ind_destinations = ind_destinations.set_index('destination')
ind_destinations.index.name = 'indicators'
ind_destinations = ind_destinations.loc[:,'unit_level_description':]

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

# Compile list of indicators
ind_matrix.sort_values('order', inplace=True)

# Create an indicators summary table
ind_matrix = ind_matrix.set_index('indicators')
ind_matrix = ind_matrix.append(ind_destinations)
ind_list = ind_matrix.index.values

indicator_tuples =  list(zip(ind_matrix.index,ind_matrix.agg_scale,ind_matrix.aggregate_description))
print("Creating Mesh Block level indicator table 'area_indicators_mb_json' (JSON nested summary statistics for each indicator at Mesh Block level)... "),
sql = '''
DROP TABLE IF EXISTS area_indicators_mb_json;
CREATE TABLE area_indicators_mb_json AS
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
           string_agg(DISTINCT(p.study_region),',')::varchar study_region,
           string_agg(DISTINCT(p.locale),',')::varchar locale,
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
  AND sample_count > 0
GROUP BY a.mb_code_2016,
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
# # The above can be used like so:
# ## SELECT mb_code_2016,sample_count,sample_count_per_ha,jsonb_pretty(indicators) AS indicators FROM abs_indicators LIMIT 1;
# ## 
# ## SELECT mb_code_2016, 
# ##        inds
# ## FROM abs_indicators, 
# ##     jsonb_array_elements(indicators) inds
# ## LIMIT 10;
# ## 
# ## SELECT mb_code_2016,
# ##        (ind->'walk_16')::jsonb->'mean' AS walk_16
# ## FROM abs_indicators,jsonb_array_elements(indicators) ind LIMIT 1;

# ## sql = '''
# ## SELECT mb_code_2016,
# ##        {extract}
# ## FROM abs_indicators,jsonb_array_elements(indicators) ind LIMIT 1;
# ## '''.format(extract = ','.join(["(ind->'{i}')->'mean' AS {i}".format(i = i) for i in ind_list]))


print("Creating weighted area aggregate tables:")
# for area in analysis_regions:  
for area in ['SA1']:  
    area_id = df_regions.loc[area,'id']
    abbrev = df_regions.loc[area,'abbreviation']
    for standard in ['dwelling','person']:
        print("  - li_inds_{}_{}".format(abbrev,standard))
        sql = '''
        DROP TABLE IF EXISTS {abbrev}_ind_{standard};
        DROP TABLE IF EXISTS li_inds_{abbrev}_{standard};
        CREATE TABLE li_inds_{abbrev}_{standard} AS
        SELECT 
        {area_code},
        study_region,
        locale,
        SUM(dwelling) AS dwelling,
        SUM(person) AS person,
        SUM(sample_count) AS sample_count,
        SUM(sample_count)/SUM(area_ha) AS sample_count_per_ha,
        SUM(area_ha) AS area_ha,
        {extract},
        ST_Union(geom) AS geom
        FROM area_indicators_mb_json,
             jsonb_array_elements(indicators) ind
        GROUP BY {area_code},study_region,locale;
        ALTER TABLE  li_inds_{abbrev}_{standard} ADD PRIMARY KEY ({area_code});
        '''.format(area_code = area_id,
                   abbrev = abbrev,
                   extract = ','.join(['''
                       (CASE             
                            -- if there are no units (dwellings or persons) the indicator is null
                            WHEN COALESCE(SUM({standard}),0) = 0
                                THEN NULL
                            -- else, calculate the value of the unit weighted indicator
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
# # create study region result
# for standard in ['dwelling','person']:
    # print("  - li_inds_{}_{}".format('study_region',standard))
    # sql = '''
    # DROP TABLE IF EXISTS {abbrev}_ind_{standard};
    # DROP TABLE IF EXISTS li_inds_{abbrev}_{standard};
    # CREATE TABLE li_inds_{abbrev}_{standard} AS
    # SELECT 
    # study_region,
    # locale,
    # SUM(dwelling) AS dwelling,
    # SUM(person) AS person,
    # SUM(sample_count) AS sample_count,
    # SUM(sample_count)/SUM(area_ha) AS sample_count_per_ha,
    # SUM(area_ha) AS area_ha,
    # {extract},
    # ST_Union(geom) AS geom
    # FROM area_indicators_mb_json,
         # jsonb_array_elements(indicators) ind
    # GROUP BY study_region,locale;
    # ALTER TABLE  li_inds_{abbrev}_{standard} ADD PRIMARY KEY (study_region);
    # '''.format(area_code = full_locale,
               # abbrev = 'study_region',
               # extract = ','.join(['''
                   # (CASE                                                       
                        # WHEN COALESCE(SUM({standard}),0) = 0
                            # THEN NULL                                          
                        # ELSE                                      
                            # ROUND(
                              # (SUM({standard}*((ind->'{i}')->>'mean')::numeric)/SUM({standard}))::numeric,
                              # {rounding}
                              # ) 
                      # END) AS "{i}"
               # '''.format(i = i,rounding=1,standard = standard) for i in ind_list]),
               # standard = standard
               # )
    # curs.execute(sql)
    # conn.commit()
# print("Done.")

# # Distribution summaries for plotting of sample data
# ind_avg = ',\n'.join("AVG(" + ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*') + '"' + ind_list+ '"' + " ) AS " +  '"' + ind_list+ '"')

# ind_sd = ',\n'.join("stddev_samp(" + ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*') + '"' + ind_list+ '"' + " ) AS " + '"' + ind_list+ '"')

# # Create query for indicator range (including scaling of percent variables)
# ind_range = ',\n'.join("ROUND(MIN(" +
                       # ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*') +
                       # '"' + ind_list+ '"' +
                       # ")::numeric,1)::text || ' to ' ||ROUND(MAX(" +
                       # ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*') +
                       # '"' + ind_list+ '"' +
                       # ")::numeric,1)::text AS " + '"'+ 
                       # ind_list+ '"' )
# # Create query for median       
# ind_median = ',\n'.join("round(percentile_cont(0.5) WITHIN GROUP (ORDER BY " +
                        # ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*')+ 
                        # '"' + ind_list + '"' + 
                       # ")::numeric,1) AS " + 
                       # '"' + ind_list+ '"')                       
                       
# # Create query for Interquartile range interval (25% to 75%) to represent the range within which the middle 50% of observations lie                       
# ind_iqr = ',\n'.join("round(percentile_cont(0.25) WITHIN GROUP (ORDER BY " +
                       # ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*') +
                       # '"' + ind_list+ '"' +
                       # ")::numeric,1)::text || ' to ' ||round(percentile_cont(0.75) WITHIN GROUP (ORDER BY " +
                       # ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*') +
                       # '"' + ind_list+ '"' +
                       # ")::numeric,1)::text AS " +
                       # '"' + ind_list+ '"')                  

# # Create a second pass table including binary indicators
# ## TO DO

# # Create query for percentile           
# ind_percentile = ',\n'.join("round(100*cume_dist() OVER(ORDER BY "+
                            # '"' + ind_list+ '"'
                            # " " +
                            # ind_matrix['polarity'] +
                            # ")::numeric,0) AS " +
                            # '"' + ind_list+ '"')        

# # Map query for raw indicators
# map_ind_raw = ',\n'.join("round(raw." +
                         # '"' + ind_list+ '"' +
                         # '::numeric,1) AS "r_' + ind_list+ '"')   
                         
# # Map query for sd indicators
# map_ind_sd = ',\n'.join("round(sd." +
                         # '"' + ind_list+ '"' +
                         # '::numeric,1) AS "sd_' + ind_list+ '"')                          
 
# # Map query for percentile indicators
# map_ind_percentile = ',\n'.join("round(perc." +
                         # '"' + ind_list+ '"' + 
                          # '::numeric,1) AS "p_' + ind_list+ '"')               
 
# # Map query for range indicators
# map_ind_range = ',\n'.join("range." + 
                         # '"' + ind_list+ '"' + 
                         # ' AS "d_' + ind_list+ '"')                   
 
# # Map query for median indicators
# map_ind_median = ',\n'.join("median." + 
                         # '"' + ind_list+ '"' + 
                         # ' AS "med_' + ind_list+ '"') 
                         
# # Map query for iqr indicators
# map_ind_iqr = ',\n'.join("iqr." + 
                         # '"' + ind_list+ '"' + 
                         # ' AS "m_' + ind_list+ '"') 

# # Exclusion criteria              
# exclusion_criteria = 'WHERE  p.exclude IS NULL AND p.sos_name_2016 IS NOT NULL'.format(id = points_id.lower())

# # The shape file for map features are output 
# map_features_outpath = os.path.join(folderPath,'study_region','wgs84_epsg4326','map_features')

# if not os.path.exists(map_features_outpath):
  # os.makedirs(map_features_outpath)   

      
      
# # SQL Settings
# conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
# curs = conn.cursor()

# areas = {'mb_code_2016':'mb',
         # 'sa1_maincode_2016':'sa1',
         # 'ssc_name_2016':'ssc',
         # 'lga_name_2016':'lga',
         # 'sos_name_2016':'sos',
         # 'study_region':'study_region'}
         
# # create aggregated raw liveability estimates for selected area
# print("Create area tables based on unweighted sample data... ")
# for area_code in areas.keys():
  # area = areas[area_code]

  # if area == 'study_region':
    # print("  {}".format("Study region"))
  # else:
    # print("  {}".format(area.upper()))
  
  # print("    - aggregate indicator table li_inds_{}... ".format(area)),
  # createTable = '''
  # DROP TABLE IF EXISTS li_inds_{area} ; 
  # CREATE TABLE li_inds_{area} AS
  # SELECT p.{area_code},
    # COUNT(*) AS sample_point_count,
    # {indicators}
    # FROM parcel_indicators p
    # LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    # {exclusion}
    # GROUP BY p.{area_code}
    # ORDER BY p.{area_code} ASC;
  # ALTER TABLE li_inds_{area} ADD PRIMARY KEY ({area_code});
  # '''.format(area = area,
             # area_code = area_code,
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
  # SELECT p.{area_code},
    # {indicators}     
    # FROM parcel_indicators p
    # LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    # {exclusion}
    # GROUP BY p.{area_code}
    # ORDER BY p.{area_code} ASC;
  # ALTER TABLE li_sd_{area} ADD PRIMARY KEY ({area_code});
  # '''.format(area = area,
             # area_code = area_code,
             # indicators = ind_sd,
             # exclusion = exclusion_criteria)
  # curs.execute(createTable)
  # conn.commit()
  # print("Done.")
  
  # print("    - range summary table li_range_{}... ".format(area)),
  # createTable = '''
  # DROP TABLE IF EXISTS li_range_{area} ; 
  # CREATE TABLE li_range_{area} AS
  # SELECT p.{area_code},
    # {indicators}     
    # FROM parcel_indicators p
    # LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    # {exclusion}
    # GROUP BY p.{area_code}
    # ORDER BY p.{area_code} ASC;
  # ALTER TABLE li_range_{area} ADD PRIMARY KEY ({area_code});
  # '''.format(area = area,
             # area_code = area_code,
             # indicators = ind_range,
             # exclusion = exclusion_criteria)
  # curs.execute(createTable)
  # conn.commit()
  # print("Done.")
  
  # print("    - median summary table li_median_{}... ".format(area)),  
  # createTable = '''
  # DROP TABLE IF EXISTS li_median_{area} ; 
  # CREATE TABLE li_median_{area} AS
  # SELECT p.{area_code},
    # {indicators}     
    # FROM parcel_indicators p
    # LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    # {exclusion}
    # GROUP BY p.{area_code}
    # ORDER BY p.{area_code} ASC;
  # ALTER TABLE li_median_{area} ADD PRIMARY KEY ({area_code});
  # '''.format(area = area,
             # area_code = area_code,
             # indicators = ind_median,
             # exclusion = exclusion_criteria)
  # curs.execute(createTable)
  # conn.commit()
  # print("Done.")
  
  # print("    - IQR summary table li_iqr_{}... ".format(area)),  
  # createTable = '''
  # DROP TABLE IF EXISTS li_iqr_{area} ; 
  # CREATE TABLE li_iqr_{area} AS
  # SELECT p.{area_code},
    # {indicators}     
    # FROM parcel_indicators p
    # LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    # {exclusion}
    # GROUP BY p.{area_code}
    # ORDER BY p.{area_code} ASC;
  # ALTER TABLE li_iqr_{area} ADD PRIMARY KEY ({area_code});
  # '''.format(area = area,
             # area_code = area_code,
             # indicators = ind_iqr,
             # exclusion = exclusion_criteria)
  # curs.execute(createTable)
  # conn.commit()
  # print("Done.")
  
  # map_ind_percentile_area = ''
  # # if area != 'study_region':
  # print("    - percentile summary table li_percentiles_{}... ".format(area)),      
  # createTable = '''
    # DROP TABLE IF EXISTS li_percentiles_{area} ; 
    # CREATE TABLE li_percentiles_{area} AS
    # SELECT {area_code},
           # {indicators}
    # FROM li_inds_{area}
    # ORDER BY {area_code} ASC;
  # ALTER TABLE li_percentiles_{area} ADD PRIMARY KEY ({area_code});
  # '''.format(area = area,
             # area_code = area_code,
             # indicators = ind_percentile)
  # curs.execute(createTable)
  # conn.commit()
  # print("Done.")
  # map_ind_percentile_area = '\n{},'.format(map_ind_percentile)
    
  # if area_code != 'mb_code_2016':
    # # Create shape files for interactive map visualisation
    # area_strings   = {'sa1_maincode_2016' :'''area.sa1_maincode_2016 AS sa1   ,\n area.suburb ,\n area.lga ,area.sample_count, area.dwelling, area.person \n''',
                      # 'ssc_name_2016':''' '-'::varchar AS sa1 ,\n area.suburb AS suburb,\n area.lga AS lga , area.sample_count, area.dwelling, area.person \n''',
                      # 'lga_name_2016':''' '-'::varchar AS sa1 ,\n '-'::varchar AS suburb ,\n area.lga  AS lga ,area.sample_count, area.dwelling, area.person \n''',
                      # 'sos_name_2016':''' '-'::varchar AS sa1 ,\n '-'::varchar AS suburb ,\n '-'  AS lga , area.sos_name_2016 AS sos \n''',
                      # 'study_region':'''area.study_region'''.format(full_locale)
                      # }      

    # area_tables = {'sa1_maincode_2016' :'''(SELECT a.sa1_maincode_2016, a.sample_count, a.person, a.dwelling, a.geom, string_agg(DISTINCT(l.ssc_name_2016),', ') AS suburb, string_agg(DISTINCT(l.lga_name_2016),', ') AS lga FROM li_inds_sa1_dwelling a LEFT JOIN mb_dwellings l USING (sa1_maincode_2016) GROUP BY a.sa1_maincode_2016,a.sample_count, a.person, a.dwelling,a.geom)''',
                   # 'ssc_name_2016':'''(SELECT a.ssc_name_2016 AS suburb, a.sample_count, a.person, a.dwelling, a.geom, string_agg(DISTINCT(lga_name_2016),', ') AS lga FROM li_inds_ssc_dwelling a LEFT JOIN mb_dwellings l USING (ssc_name_2016) GROUP BY a.ssc_name_2016,a.sample_count, a.person, a.dwelling,a.geom)''',
                   # 'lga_name_2016':'''(SELECT a.lga_name_2016 AS lga_name_2016, a.sample_count, a.person, a.dwelling, a.geom, string_agg(DISTINCT(ssc_name_2016),', ') AS suburb FROM li_inds_lga_dwelling a LEFT JOIN mb_dwellings l USING (lga_name_2016) GROUP BY a.lga_name_2016,a.sample_count, a.person, a.dwelling,a.geom)''',
                   # 'sos_name_2016': 'study_region_urban',
                   # 'study_region': ''' (SELECT study_region,locale, ST_Union(geom) AS geom FROM area_indicators_mb_json GROUP BY study_region,locale) '''.format(full_locale)}   
                  
    # area_code_tables = {'sa1_maincode_2016' :'''LEFT JOIN sa1_2016_aust AS area_code ON area.sa1_maincode_2016 = area_code.sa1_maincode_2016''',
                        # 'ssc_name_2016':     '''LEFT JOIN ssc_2016_aust AS area_code ON area.suburb            = area_code.ssc_name_2016''',
                        # 'lga_name_2016':     '''LEFT JOIN lga_2016_aust AS area_code ON area.lga               = area_code.lga_name_2016''',
                        # 'sos_name_2016':     ''' ''',
                        # 'study_region': ''' '''}
                      
    # community_code = {'sa1_maincode_2016' :'''area_code.sa1_7digitcode_2016 AS community_code''',
                      # 'ssc_name_2016':'''CONCAT('SSC',area_code.ssc_code_2016::varchar) AS community_code''',
                      # 'lga_name_2016':'''CONCAT('LGA',area_code.lga_code_2016::varchar) AS community_code''',
                      # 'sos_name_2016':''' '-'::varchar ''',
                      # 'study_region':''' '-'::varchar '''}

    # boundary_tables = {'sa1_maincode_2016' :'''sa1_2016_aust b WHERE b.sa1_maincode_2016 IN (SELECT sa1_maincode_2016 FROM area_sa1_included) ''',
                       # 'ssc_name_2016':     '''ssc_2016_aust b WHERE b.ssc_name_2016 IN (SELECT ssc_name_2016 FROM area_ssc_included) ''',
                       # 'lga_name_2016':     '''lga_2016_aust b WHERE b.lga_name_2016 IN (SELECT lga_name_2016 FROM area_lga_included) ''',
                       # 'sos_name_2016': '''study_region_urban b ''',
                       # 'study_region': ''' (SELECT '{}'::varchar AS study_region, geom FROM study_region_urban WHERE urban = 'urban') b '''.format(full_locale)}
                       
    # percentile_join_string = ' '              
    # # if area != 'region':
    # percentile_join_string = '''
      # LEFT JOIN li_percentiles_{area} AS perc 
             # ON area.{area_code} = perc.{area_code}
      # '''.format(area = area,area_code = area_code)

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
    # LEFT JOIN li_inds_{area} AS raw ON area.{area_code} = raw.{area_code}
    # {percentile_join}
    # LEFT JOIN li_range_{area} AS range ON area.{area_code} = range.{area_code}
    # LEFT JOIN li_iqr_{area} AS iqr ON area.{area_code} = iqr.{area_code}
    # {area_code_table};
    # '''.format(area = area,
               # area_code = area_code,
               # area_table = area_tables[area_code],
               # area_strings = area_strings[area_code],
               # raw = map_ind_raw,
               # sd = map_ind_sd,
               # percentile = map_ind_percentile_area,
               # range = map_ind_range,
               # median = map_ind_median,
               # iqr = map_ind_iqr,
               # community_code = community_code[area_code],
               # area_code_table = area_code_tables[area_code],
               # percentile_join = percentile_join_string)
    # print("    - Create table for mapping indicators at {} level".format(area))
    # # print(createTable)
    # curs.execute(createTable)
    # conn.commit()
    
    # createTable = '''
    # DROP TABLE IF EXISTS boundaries_{area};
    # CREATE TABLE boundaries_{area} AS
    # SELECT b.{area_code},
            # ST_Transform(b.geom,4326) AS geom         
    # FROM {boundaries};
    # '''.format(area = area,
               # area_code = area_code,
               # boundaries = boundary_tables[area_code])
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
# conn.close()
  
# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
