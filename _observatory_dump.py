# Purpose: Export indicators for the Australian Urban Observatory
# Author:  Carl Higgs 
# Date:    20 July 2018

import os
import sys
import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine
from datetime import datetime

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create area level indicator tables for {}'.format(locale)

today = datetime.today().strftime('%Y-%m-%d')

if 'nodrop' in sys.argv:
    drop = '--'
else:
    drop = ''

# Connect to postgresql database     
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))


# Restrict to indicators associated with study region (except distance to closest dest indicators)
# the following two tables (indicators/measures, and distances to closest measures) will later be
# appended once the first table is expanded into soft and hard threshold indicator forms
ind_destinations = df_destinations[(df_destinations.locale == "*") | (df_destinations.locale == locale)].copy()
ind_destinations['destination'] = ind_destinations['destination_class'].apply(lambda x: "dist_m_{}".format(x))
ind_destinations = ind_destinations.set_index('destination')
ind_destinations.index.name = 'indicators'
ind_destinations = ind_destinations.loc[:,'unit_level_description':]
ind_destinations['scale'] = 'point'
# Indicator configuration sheet is 'df_inds', read in from config file in the config script
# Restrict to indicators associated with study region (except distance to closest dest indicators)
ind_matrix = df_inds[df_inds['locale'].str.contains('|'.join([locale,'\*']))].copy()

# # get the set of distance to closest regions which match for this region
# destinations = df_inds[df_inds['ind'].str.contains('destinations')]
# current_categories = [x for x in categories if 'distance_m_{}'.format(x) in destinations.ind_plain.str.encode('utf8').tolist()]
# ind_matrix = ind_matrix.append(destinations[destinations['ind_plain'].str.replace('distance_m_','').str.contains('|'.join(current_categories))])
ind_matrix['order'] = ind_matrix.index
ind_soft = ind_matrix.loc[ind_matrix.tags=='_{threshold}',:].copy()
ind_hard = ind_matrix.loc[ind_matrix.tags=='_{threshold}',:].copy()
ind_soft.replace(to_replace='{threshold}', value='soft', inplace=True,regex=True)
ind_hard.replace(to_replace='{threshold}', value='hard', inplace=True,regex=True)

ind_matrix = pandas.concat([ind_matrix,ind_soft,ind_hard], ignore_index=True).sort_values('ind')
ind_matrix.drop(ind_matrix[ind_matrix.tags == '_{threshold}'].index, inplace=True)
# Restrict to indicators with a defined query
ind_matrix = ind_matrix[pandas.notnull(ind_matrix['Query'])]
ind_matrix = ind_matrix[pandas.notnull(ind_matrix['updated?'])]

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

# Retain only subset of variables, as defined in spreadsheet
df_observatory = pandas.read_excel(xls, 'observatory',index_col=0)
ind_matrix = df_observatory.join(ind_matrix).copy()

# Drop index for ind_observatory table if it exists; 
# this causes an error when (re-)creating the ind_observatory table if index exists
engine.execute('''DROP INDEX IF EXISTS ix_ind_observatory_{locale}_{year}_index;'''.format(locale=locale,year=year))
ind_matrix.to_sql(name='ind_observatory_{}_{}'.format(locale,year),con=engine,if_exists='replace')

# Expand out area level indicators
#   - remove the signifying prefix 'area:'
def unnesting(df, explode):
    import pandas
    import numpy as np
    idx = df.index.repeat(df[explode[0]].str.len())
    df1 = pandas.concat([
        pandas.DataFrame({x: np.concatenate(df[x].values)}) for x in explode], axis=1)
    df1.index = idx
    return(df1.join(df.drop(explode, 1), how='left'))
    
ind_matrix['scale'] = ind_matrix.scale.str.replace('area:','').str.split(',')
ind_matrix = unnesting(ind_matrix,['scale'])


area_indicators = {}
area_queries = {}
area_sources = {}
for area in [x for x in set(ind_matrix.scale.values) if x!= 'point']:
    area_matrix = ind_matrix.query("scale=='point' | scale=='{}'".format(df_regions.query('id=="sa1_maincode_2016"').index[0])).copy()
    # print(area),
    abbrev = df_regions.loc[area,'abbreviation']
    # print(': {}'.format(abbrev))
    # Get index column name (e.g. for SA1 may be 7 digit or maincode - easiest to check)
    area_indicators[area] = ind_matrix.query('scale=="{}"'.format(area)).copy()
    area_indicators[area].replace(to_replace='{area}', value=abbrev, inplace=True,regex=True)
    table = 'li_inds_{area}_dwelling'.format(area=area)
    area_id = df_regions.loc[area,'id']
    for ind in area_indicators[area].index:
        area_sources[area] =  'LEFT JOIN {table} area_inds ON area.{area_id} = {table}.{area_id}'.format(area=area,table = table,area_id=area_id)
        area_indicators[area].loc[ind,'Query'] = 'area_inds.{ind}'.format(ind=ind)
    else:
        area_queries[area] = ''
        area_sources[area] = ''

# Exclusion criteria              
exclusion_criteria = 'WHERE  p.exclude IS NULL AND p.sos_name_2016 IS NOT NULL'.format(id = points_id.lower())

# # The shape file for map features are output 
map_features_outpath = os.path.join(folderPath,'observatory',today)

if not os.path.exists(map_features_outpath):
  os.makedirs(map_features_outpath)   
      
# SQL Settings
areas = {'mb_code_2016':'mb',
         'sa1_maincode_2016':'sa1',
         'ssc_name_2016':'ssc',
         'lga_name_2016':'lga',
         'sos_name_2016':'sos',
         'study_region':'region'}
  
# create aggregated raw liveability estimates for selected area
print("Create area tables based on unweighted sample data... ")


# Note issue in June 2020 dump where ind name of area indicators isn't as per schema (e.g. hous_07) but as of ind_plain (e.g. pct_social_housing); that won't work for obs; we'll do an ad hoc fix here as time is running out
point_ind_list = ind_matrix.query("scale=='point'").index.values
point_inds = ','.join(point_ind_list)
for area_code in areas.keys():
    area = areas[area_code]
    area_matrix = ind_matrix.query("scale=='point' | scale=='{}'".format(df_regions.query('id=="{area_code}"'.format(area_code=area_code)).index[0])).copy()
  
    # Distribution summaries for plotting of sample data
    ind_avg = ',\n'.join(area_matrix.apply(lambda x: 'AVG({mult}*p."{i}") AS "{i}"'.format(i=x.name,mult='100.0' if x['agg_scale']==100 else '1.0')
                                                    if x.scale=='point' else 'area_inds."{p}" AS "{i}"'.format(p=x.ind_plain, i=x.name),axis=1))
    ind_sd = ',\n'.join(area_matrix.apply(lambda x: 'stddev_samp({mult}*p."{i}") AS "{i}"'.format(i=x.name,mult='100.0' if x['agg_scale']==100 else '1.0')
                                                    if x.scale=='point' else 'NULL AS "{i}"'.format(i=x.name),axis=1))
    
    # Create query for indicator range (including scaling of percent variables)
    ind_range = ',\n'.join(area_matrix.apply(lambda x: 'ROUND(MIN({mult}*p."{ind}")::numeric,1)::text || $$ to $$ ||ROUND(MAX({mult}* p."{ind}")::numeric,1)::text AS "{ind}"'.format(ind=x.name,mult = '100.0' if x.agg_scale == 100 else '1.0')
                                                    if x.scale=='point' else 'NULL AS "{i}"'.format(i=x.name),axis=1))
    
    # Create query for median       
    ind_median = ',\n'.join(area_matrix.apply(lambda x: 'round(percentile_cont(0.5) WITHIN GROUP (ORDER BY {mult}*p."{ind}" )::numeric,1) AS "{ind}"'.format(ind=x.name,mult = '100.0' if x.agg_scale == 100 else '1.0')
                                                    if x.scale=='point' else 'NULL AS "{i}"'.format(i=x.name),axis=1))
    
    # Create query for Interquartile range interval (25% to 75%) to represent the range within which the middle 50% of observations lie         
    ind_iqr = ',\n'.join(area_matrix.apply(lambda x: 'round(percentile_cont(0.25) WITHIN GROUP (ORDER BY {mult}*p."{ind}" )::numeric,1)::text || $$ to $$ || round(percentile_cont(0.75) WITHIN GROUP (ORDER BY {mult}*p."{ind}" )::numeric,1)::text AS "{ind}"'.format(ind=x.name,mult = '100.0' if x.agg_scale == 100 else '1.0')
                                                    if x.scale=='point' else 'NULL AS "{i}"'.format(i=x.name),axis=1))
    
    # Create query for percentile           
    ind_percentile = ',\n'.join(area_matrix.apply(lambda x: 'round(100*cume_dist() OVER(ORDER BY p."{ind}" {polarity})::numeric,0) AS "{ind}"'.format(ind=x.name,polarity = x.polarity)
                                                    if x.scale=='point' else 'NULL AS "{i}"'.format(i=x.name),axis=1))
    
    # Map indicator queries
    map_ind_raw = ',\n'.join(area_matrix.apply(lambda x: 'round(raw."{ind}"::numeric,1) AS "r_{ind}"'.format(ind=x.name),axis=1))                           
    map_ind_sd = ',\n'.join(area_matrix.apply(lambda x: 'round(sd."{ind}"::numeric,1) AS "sd_{ind}"'.format(ind=x.name),axis=1))
    map_ind_percentile = ',\n'.join(area_matrix.apply(lambda x: 'round(perc."{ind}"::numeric,1) AS "p_{ind}"'.format(ind=x.name),axis=1))    
    map_ind_range = ',\n'.join(area_matrix.apply(lambda x: 'range."{ind}" AS "d_{ind}"'.format(ind=x.name),axis=1))  
    map_ind_median = ',\n'.join(area_matrix.apply(lambda x: 'median."{ind}" AS "med_{ind}"'.format(ind=x.name),axis=1))  
    map_ind_iqr = ',\n'.join(area_matrix.apply(lambda x: 'iqr."{ind}" AS "m_{ind}"'.format(ind=x.name),axis=1))  
    
    if area == 'region':
      print("  {}".format("Study region"))
    else:
      print("  {}".format(area.upper()))
    
    print("    - aggregate indicator table observatory_inds_{}... ".format(area)),
    ### NOTE: We use dwelling weighted average for Observatory (and other) purposes; this is an unweighted table
    createTable = '''
    {drop} DROP TABLE IF EXISTS observatory_inds_{area} ; 
    CREATE TABLE  IF NOT EXISTS observatory_inds_{area} AS
    SELECT area_inds.{area_code},
      COUNT(*) AS sample_point_count,
      {indicators}
      FROM li_inds_{area}_dwelling area_inds 
      LEFT JOIN (SELECT i.{area_code},
                        i.sos_name_2016,
                        i.exclude,
                          {point_inds}
                   FROM parcel_indicators i
              LEFT JOIN dest_closest_indicators USING (gnaf_pid)
                 ) p USING ({area_code})
      {exclusion}
      GROUP BY area_inds.{area_code}
      ORDER BY area_inds.{area_code} ASC;
    CREATE INDEX IF NOT EXISTS observatory_inds_{area}_pkey ON observatory_inds_{area} ({area_code});
    '''.format(drop = drop,
               area = area,
               area_code = area_code,
               point_inds = point_inds,
               indicators = ind_avg,
               exclusion = exclusion_criteria)
    print(createTable)
    engine.execute(createTable)
    print("Done.")
    
    print("    - sd summary table observatory_sd_{}... ".format(area)),
    createTable = '''
    {drop} DROP TABLE IF EXISTS observatory_sd_{area} ; 
    CREATE TABLE  IF NOT EXISTS observatory_sd_{area} AS
    SELECT p.{area_code},
      {indicators}     
      FROM parcel_indicators p
      LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
      {exclusion}
      GROUP BY p.{area_code}
      ORDER BY p.{area_code} ASC;
    CREATE INDEX IF NOT EXISTS observatory_sd_{area}_pkey ON observatory_sd_{area} ({area_code});
    '''.format(drop = drop,
               area = area,
               area_code = area_code,
               indicators = ind_sd,
               exclusion = exclusion_criteria)
    engine.execute(createTable)
    print("Done.")
    
    print("    - range summary table observatory_range_{}... ".format(area)),
    createTable = '''
    {drop} DROP TABLE IF EXISTS observatory_range_{area} ; 
    CREATE TABLE  IF NOT EXISTS observatory_range_{area} AS
    SELECT p.{area_code},
      {indicators}     
      FROM parcel_indicators p
      LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
      {exclusion}
      GROUP BY p.{area_code}
      ORDER BY p.{area_code} ASC;
    CREATE INDEX IF NOT EXISTS observatory_range_{area}_pkey ON observatory_range_{area} ({area_code});
    '''.format(drop = drop,
               area = area,
               area_code = area_code,
               indicators = ind_range,
               exclusion = exclusion_criteria)
    engine.execute(createTable)
    print("Done.")
    
    print("    - median summary table observatory_median_{}... ".format(area)),  
    createTable = '''
    {drop} DROP TABLE IF EXISTS observatory_median_{area} ; 
    CREATE TABLE  IF NOT EXISTS observatory_median_{area} AS
    SELECT p.{area_code},
      {indicators}     
      FROM parcel_indicators p
      LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
      {exclusion}
      GROUP BY p.{area_code}
      ORDER BY p.{area_code} ASC;
    CREATE INDEX IF NOT EXISTS observatory_median_{area}_pkey ON observatory_median_{area} ({area_code});
    '''.format(drop = drop,
               area = area,
               area_code = area_code,
               indicators = ind_median,
               exclusion = exclusion_criteria)
    engine.execute(createTable)
    print("Done.")
    
    print("    - IQR summary table observatory_iqr_{}... ".format(area)),  
    createTable = '''
    {drop} DROP TABLE IF EXISTS observatory_iqr_{area} ; 
    CREATE TABLE  IF NOT EXISTS observatory_iqr_{area} AS
    SELECT p.{area_code},
      {indicators}     
      FROM parcel_indicators p
      LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
      {exclusion}
      GROUP BY p.{area_code}
      ORDER BY p.{area_code} ASC;
    CREATE INDEX IF NOT EXISTS observatory_iqr_{area}_pkey ON observatory_iqr_{area} ({area_code});
    '''.format(drop = drop,
               area = area,
               area_code = area_code,
               indicators = ind_iqr,
               exclusion = exclusion_criteria)
    engine.execute(createTable)
    print("Done.")
    
    map_ind_percentile_area = ''
    # if area != 'study_region':
    if area == 'region':
        sr = ''
    else:
        sr = 'study_region,'
    print("    - percentile summary table observatory_percentiles_{}... ".format(area)),      
    createTable = '''
      {drop} DROP TABLE IF EXISTS observatory_percentiles_{area} ; 
      CREATE TABLE IF NOT EXISTS observatory_percentiles_{area} AS
      SELECT {area_code},
              {study_region}
             {indicators}
      FROM li_inds_{area}_dwelling
      ORDER BY {area_code} ASC;
    CREATE INDEX IF NOT EXISTS observatory_percentiles_{area}_pkey ON observatory_percentiles_{area} ({area_code});
    '''.format(drop = drop,
               area = area,
               area_code = area_code,
               study_region = sr,
               indicators = ind_percentile)
    engine.execute(createTable)
    print("Done.")
    map_ind_percentile_area = '\n{},'.format(map_ind_percentile)
      
    if area_code != 'mb_code_2016':
        # Create shape files for interactive map visualisation
        area_strings   = {'sa1_maincode_2016' :'''area.study_region,area.sa1_maincode_2016 AS sa1   ,\n area.suburb ,\n area.lga ,area.sample_count, area.dwelling, area.person \n''',
                          'ssc_name_2016':'''area.study_region,'-'::varchar AS sa1 ,\n area.ssc_name_2016 AS suburb,\n area.lga AS lga , area.sample_count, area.dwelling, area.person \n''',
                          'lga_name_2016':'''area.study_region,'-'::varchar AS sa1 ,\n '-'::varchar AS suburb ,\n area.lga_name_2016  AS lga ,area.sample_count, area.dwelling, area.person \n''',
                          'sos_name_2016':'''raw.study_region,'-'::varchar AS sa1 ,\n '-'::varchar AS suburb ,\n '-'  AS lga , area.sos_name_2016 AS sos \n''',
                          'study_region':'''area.study_region,'-'::varchar AS sa1 ,\n '-'::varchar AS suburb, area.sample_count, area.dwelling, area.person \n'''
                          }      
        
        area_tables = {'sa1_maincode_2016' :'''
           (SELECT a.sa1_maincode_2016, 
                   a.study_region, 
                   a.sample_count, 
                   a.person, 
                   a.dwelling, 
                   a.geom, 
                   string_agg(DISTINCT(l.ssc_name_2016),', ') AS suburb, 
                   string_agg(DISTINCT(l.lga_name_2016),', ') AS lga 
           FROM li_inds_sa1_dwelling a 
           LEFT JOIN area_linkage l USING (sa1_maincode_2016) 
           GROUP BY a.sa1_maincode_2016,a.study_region,a.sample_count, a.person, a.dwelling,a.geom)''',
           'ssc_name_2016':'''
           (SELECT a.ssc_name_2016, 
                   a.study_region,
                   a.sample_count, 
                   a.person, 
                   a.dwelling, 
                   a.geom, 
                   string_agg(DISTINCT(lga_name_2016),', ') AS lga 
           FROM li_inds_ssc_dwelling a 
           LEFT JOIN area_linkage l USING (ssc_name_2016) 
           GROUP BY a.ssc_name_2016,a.study_region,a.sample_count, a.person, a.dwelling,a.geom)''',
            'lga_name_2016':'''
           (SELECT a.lga_name_2016, 
                   a.study_region,
                   a.sample_count, 
                   a.person, 
                   a.dwelling, 
                   a.geom, 
                   string_agg(DISTINCT(ssc_name_2016),', ') AS suburb 
           FROM li_inds_lga_dwelling a 
           LEFT JOIN area_linkage l USING (lga_name_2016) 
           GROUP BY a.lga_name_2016,a.study_region,a.sample_count, a.person, a.dwelling,a.geom)''',
           'sos_name_2016': 'study_region_all_sos',
           'study_region':'''
           (SELECT study_region, 
                   sample_count, 
                   person, 
                   dwelling, 
                   geom
                   FROM li_inds_region_dwelling)'''}   
                      
        area_code_tables = {'sa1_maincode_2016' :'''LEFT JOIN sa1_2016_aust AS area_code ON area.sa1_maincode_2016 = area_code.sa1_maincode_2016''',
                            'ssc_name_2016':     '''LEFT JOIN ssc_2016_aust AS area_code ON area.ssc_name_2016 = area_code.ssc_name_2016''',
                            'lga_name_2016':     '''LEFT JOIN lga_2016_aust AS area_code ON area.lga_name_2016 = area_code.lga_name_2016''',
                            'sos_name_2016':     ''' ''',
                            'study_region': ''' '''}
                          
        community_code = {'sa1_maincode_2016' :'''area_code.sa1_7digitcode_2016 AS community_code''',
                          'ssc_name_2016':'''CONCAT('SSC',area_code.ssc_code_2016::varchar) AS community_code''',
                          'lga_name_2016':'''CONCAT('LGA',area_code.lga_code_2016::varchar) AS community_code''',
                          'sos_name_2016':''' '-'::varchar ''',
                          'study_region':''' '-'::varchar '''}
        
        boundary_tables = {'sa1_maincode_2016' :'''sa1_2016_aust b WHERE b.sa1_maincode_2016 IN (SELECT sa1_maincode_2016 FROM area_sa1_included) ''',
                           'ssc_name_2016':     '''ssc_2016_aust b WHERE b.ssc_name_2016 IN (SELECT ssc_name_2016 FROM area_ssc_included) ''',
                           'lga_name_2016':     '''lga_2016_aust b WHERE b.lga_name_2016 IN (SELECT lga_name_2016 FROM area_lga_included) ''',
                           'sos_name_2016': ''' study_region_all_sos b ''',
                           'study_region': ''' (SELECT '{}'::varchar AS study_region, geom FROM study_region_urban WHERE urban = 'urban') b '''.format(full_locale)}
        
        primary_key = {'sa1_maincode_2016':'''sa1''',
                           'ssc_name_2016':'''suburb''',
                           'lga_name_2016':'''lga''',
                           'sos_name_2016':'''"sos"''',
                           'study_region': '''study_region'''}
                           
        percentile_join_string = ' '              
        # if area != 'region':
        percentile_join_string = '''
          LEFT JOIN observatory_percentiles_{area} AS perc 
                 ON area.{area_code} = perc.{area_code}
          '''.format(area = area,area_code = area_code)
        
        # Note -i've excerpted SD and median out of the below table for now; too much for SA1s  
        #        {sd},
        #        {median},
        # LEFT JOIN observatory_median_{area} AS median ON area.{area_code2} = median.{area_code}
        # LEFT JOIN observatory_sd_{area} AS sd ON area.{area_code2} = raw.{area_code}
        createTable = '''
        {drop} DROP TABLE IF EXISTS observatory_map_{area}_{locale}_{year};
        CREATE TABLE IF NOT EXISTS observatory_map_{area}_{locale}_{year} AS
        SELECT {area_strings},
               {raw},
               {percentile}
               {range},
               {iqr},
               {community_code},
               ST_Transform(ST_Simplify(area.geom,.1),4326) AS geom              
        FROM {area_table} AS area
        -- note I join up here with the dwelling weighted average
        LEFT JOIN li_inds_{area}_dwelling AS raw ON area.{area_code} = raw.{area_code}
        {percentile_join}
        LEFT JOIN observatory_range_{area} AS range ON area.{area_code} = range.{area_code}
        LEFT JOIN observatory_iqr_{area} AS iqr ON area.{area_code} = iqr.{area_code}
        {area_code_table};
        CREATE INDEX IF NOT EXISTS observatory_map_{area}_{locale}_{year}_pkey ON observatory_map_{area}_{locale}_{year} ({primary_key});
        CREATE INDEX IF NOT EXISTS observatory_map_{area}_{locale}_{year}_study_region ON observatory_map_{area}_{locale}_{year} (study_region);;
        '''.format(drop = drop,
                   area = area,
                   locale = locale,
                   year = year,
                   area_code = area_code,
                   area_table = area_tables[area_code],
                   area_strings = area_strings[area_code],
                   raw = map_ind_raw,
                   sd = map_ind_sd,
                   percentile = map_ind_percentile_area,
                   range = map_ind_range,
                   median = map_ind_median,
                   iqr = map_ind_iqr,
                   community_code = community_code[area_code],
                   area_code_table = area_code_tables[area_code],
                   percentile_join = percentile_join_string,
                   primary_key = primary_key[area_code])
        print("    - Create table for mapping indicators at {} level".format(area))
        # print(createTable)
        engine.execute(createTable)
        
        createTable = '''
        {drop} DROP TABLE IF EXISTS boundaries_{area}_{locale}_{year};
        CREATE TABLE IF NOT EXISTS boundaries_{area}_{locale}_{year} AS
        SELECT b.{area_code},
               ST_Transform(ST_Simplify(b.geom,.1),4326) AS geom         
        FROM {boundaries};
        '''.format(drop = drop,
                   area = area,
                   locale = locale,
                   year = year,
                   area_code = area_code,
                   boundaries = boundary_tables[area_code])
        print("    - boundary overlays at {} level".format(area)),
        engine.execute(createTable)
        print("Done.")
        
        createTable = '''
        {drop} DROP TABLE IF EXISTS urban_sos_{area}_{locale}_{year};
        CREATE TABLE IF NOT EXISTS urban_sos_{area}_{locale}_{year} AS
        SELECT urban,
               ST_Transform(ST_Simplify(geom,.1),4326) AS geom         
        FROM study_region_urban
        WHERE urban  = 'urban';
        '''.format(drop = drop,
                   area = area,
                   locale = locale,
                   year = year,
                   area_code = area_code,
                   boundaries = boundary_tables[area_code])
        print("    - urban overlays at {} level".format(area)),
        engine.execute(createTable)
        print("Done.")

# need to add in a geometry column to ind_observatory to allow for importing of this table as a layer in geoserver
# If it doesn't already exists
# So, check if it already exists
engine.execute('''SELECT column_name FROM information_schema.columns WHERE table_name='ind_observatory_{}_{}' and column_name='geom';'''.format(locale,year))
null_geom_check = [x[0] for x in engine.execute(sql)]
if len(null_geom_check)==0:
  # if geom doesn't exist, created it
  engine.execute('''SELECT AddGeometryColumn ('public','ind_observatory_{}_{}','geom',4326,'POINT',2);'''.format(locale,year))

map_tables = ["boundaries_sos","boundaries_sa1","boundaries_ssc","boundaries_lga","observatory_map_region","observatory_map_sa1","observatory_map_ssc","observatory_map_lga","ind_observatory","boundaries_region","boundaries_sa1","boundaries_ssc","boundaries_lga"]

if locale == 'australia':
    map_tables = map_tables+['li_map_australia_{year}'.format(year=year)]
    
output_tables = ' '.join([' -t "{x}_{locale}_{year}"'.format(x = x,locale = locale,year = year) for x in map_tables]).replace('_australia_2018_australia_2018','_australia_2018')
output_tables_gpkg = ' '.join(['"{x}_{locale}_{year}"'.format(x = x,locale = locale,year = year) for x in map_tables]).replace('_australia_2018_australia_2018','_australia_2018')


print("Output to geopackage gpkg: {path}/auo_map_{db}.gpkg".format(path = map_features_outpath, db = db)),

gpkg =  '{}/auo_map_{}.gpkg'.format(map_features_outpath,db)

# delete file if exists
if os.path.exists(gpkg):
    os.remove(gpkg)

command = (
               'ogr2ogr -overwrite -f GPKG {gpkg} '
               'PG:"host={host} user={user} dbname={db} password={pwd}" '
               '{tables} ' 
               ).format(gpkg = gpkg,
                       host = db_host,
                       user = db_user,
                       pwd = db_pwd,
                       db = db,
                       tables = output_tables_gpkg) 
sp.call(command)
print("Done.")

print("\nOutput postgresql dump... ")
command = (
           'pg_dump -Fc -Z 9 {tables} postgresql://{user}:{pwd}@{host}:5432/{db} '
           '> '
           '{dir}/auo_map_{db}.sql'
           ).format(user = db_user,
                    pwd = db_pwd,
                    host = db_host,
                    dir = map_features_outpath,
                    db = db,
                    tables = output_tables)
sp.call(command, shell=True)
print('''Done; but if it actually did not work, please run the following command:\n
pg_dump -U postgres -h localhost -W  {tables} {db} > {dir}/auo_map_{db}.sql
'''.format(locale = locale.lower(), year = year,db = db,tables = output_tables,dir = map_features_outpath))

print('''

Also, can you send the following line of text to Carl please to aid collation of study regions?

psql:
    \c postgres
    DROP DATABASE IF EXISTS obs_source;
    CREATE DATABASE obs_source;
    \c obs_source
    CREATE EXTENSION postgis;
cmd:    
    pg_restore -U postgres -Fc -d obs_source < {dir}/auo_map_{db}.sql
    
If desired, the Australia-wide source tables can then be expanded for specific regions and (optionally) exported 
to a further dated sql dump using:
    
cmd:
    python _observatory_expand_australian_regions.py australia obs_source export
'''.format(dir = map_features_outpath,db = db))