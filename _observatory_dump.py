# Script:  19_area_indicators.py
# Purpose: Create area level indicator tables
# Author:  Carl Higgs 
# Date:    20 July 2018

import os
import sys
import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create area level indicator tables for {}'.format(locale)

# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

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
ind_matrix = ind_matrix.query("observatory not in ['','NULL','NaN'] and observatory.notnull()")
ind_matrix.sort_values('observatory', inplace=True)
ind_list = ind_matrix.index.values

# Drop index for ind_observatory table if it exists; 
# this causes an error when (re-)creating the ind_observatory table if index exists
curs.execute('DROP INDEX IF EXISTS ix_ind_observatory_{}_{}_index;'.format(locale,year))
conn.commit()
ind_matrix.to_sql(name='ind_observatory_{}_{}'.format(locale,year),con=engine,if_exists='replace')
ind_list = ind_matrix.index.values

# Distribution summaries for plotting of sample data
ind_avg = ',\n'.join("AVG(" + ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*') + '"' + ind_list+ '"' + " ) AS " +  '"' + ind_list+ '"')

ind_sd = ',\n'.join("stddev_samp(" + ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*') + '"' + ind_list+ '"' + " ) AS " + '"' + ind_list+ '"')

# Create query for indicator range (including scaling of percent variables)
ind_range = ',\n'.join("ROUND(MIN(" +
                       ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*') +
                       '"' + ind_list+ '"' +
                       ")::numeric,1)::text || ' to ' ||ROUND(MAX(" +
                       ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*') +
                       '"' + ind_list+ '"' +
                       ")::numeric,1)::text AS " + '"'+ 
                       ind_list+ '"' )
# Create query for median       
ind_median = ',\n'.join("round(percentile_cont(0.5) WITHIN GROUP (ORDER BY " +
                        ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*')+ 
                        '"' + ind_list + '"' + 
                       ")::numeric,1) AS " + 
                       '"' + ind_list+ '"')                       
                       
# Create query for Interquartile range interval (25% to 75%) to represent the range within which the middle 50% of observations lie                       
ind_iqr = ',\n'.join("round(percentile_cont(0.25) WITHIN GROUP (ORDER BY " +
                       ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*') +
                       '"' + ind_list+ '"' +
                       ")::numeric,1)::text || ' to ' ||round(percentile_cont(0.75) WITHIN GROUP (ORDER BY " +
                       ind_matrix['agg_scale'].apply(lambda x: '100.0*' if x == 100 else '1.0*') +
                       '"' + ind_list+ '"' +
                       ")::numeric,1)::text AS " +
                       '"' + ind_list+ '"')                  

# Create a second pass table including binary indicators
## TO DO

# Create query for percentile           
ind_percentile = ',\n'.join("round(100*cume_dist() OVER(ORDER BY "+
                            '"' + ind_list+ '"'
                            " " +
                            ind_matrix['polarity'] +
                            ")::numeric,0) AS " +
                            '"' + ind_list+ '"')        

# Map query for raw indicators
map_ind_raw = ',\n'.join("round(raw." +
                         '"' + ind_list+ '"' +
                         '::numeric,1) AS "r_' + ind_list+ '"')   
                         
# Map query for sd indicators
map_ind_sd = ',\n'.join("round(sd." +
                         '"' + ind_list+ '"' +
                         '::numeric,1) AS "sd_' + ind_list+ '"')                          
 
# Map query for percentile indicators
map_ind_percentile = ',\n'.join("round(perc." +
                         '"' + ind_list+ '"' + 
                          '::numeric,1) AS "p_' + ind_list+ '"')               
 
# Map query for range indicators
map_ind_range = ',\n'.join("range." + 
                         '"' + ind_list+ '"' + 
                         ' AS "d_' + ind_list+ '"')                   
 
# Map query for median indicators
map_ind_median = ',\n'.join("median." + 
                         '"' + ind_list+ '"' + 
                         ' AS "med_' + ind_list+ '"') 
                         
# Map query for iqr indicators
map_ind_iqr = ',\n'.join("iqr." + 
                         '"' + ind_list+ '"' + 
                         ' AS "m_' + ind_list+ '"') 

# Exclusion criteria              
exclusion_criteria = 'WHERE  p.exclude IS NULL AND p.sos_name_2016 IS NOT NULL'.format(id = points_id.lower())

# # The shape file for map features are output 
map_features_outpath = os.path.join(folderPath,'study_region','wgs84_epsg4326','map_features')

if not os.path.exists(map_features_outpath):
  os.makedirs(map_features_outpath)   

      
      
# SQL Settings
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

areas = {'mb_code_2016':'mb',
         'sa1_maincode_2016':'sa1',
         'ssc_name_2016':'ssc',
         'lga_name_2016':'lga',
         'sos_name_2016':'sos',
         'study_region':'region'}
  
# create aggregated raw liveability estimates for selected area
print("Create area tables based on unweighted sample data... ")
for area_code in areas.keys():
  area = areas[area_code]

  if area == 'study_region':
    print("  {}".format("Study region"))
  else:
    print("  {}".format(area.upper()))
  
  print("    - aggregate indicator table li_inds_{}... ".format(area)),
  ### NOTE: We use dwelling weighted average for Observatory (and other) purposes; this is an unweighted table
  createTable = '''
  DROP TABLE IF EXISTS li_inds_{area} ; 
  CREATE TABLE li_inds_{area} AS
  SELECT p.{area_code},
    COUNT(*) AS sample_point_count,
    {indicators}
    FROM parcel_indicators p
    LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    {exclusion}
    GROUP BY p.{area_code}
    ORDER BY p.{area_code} ASC;
  ALTER TABLE li_inds_{area} ADD PRIMARY KEY ({area_code});
  '''.format(area = area,
             area_code = area_code,
             indicators = ind_avg,
             exclusion = exclusion_criteria)
  curs.execute(createTable)
  conn.commit()
  print("Done.")
  
  print("    - sd summary table li_sd_{}... ".format(area)),
  createTable = '''
  DROP TABLE IF EXISTS li_sd_{area} ; 
  CREATE TABLE li_sd_{area} AS
  SELECT p.{area_code},
    {indicators}     
    FROM parcel_indicators p
    LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    {exclusion}
    GROUP BY p.{area_code}
    ORDER BY p.{area_code} ASC;
  ALTER TABLE li_sd_{area} ADD PRIMARY KEY ({area_code});
  '''.format(area = area,
             area_code = area_code,
             indicators = ind_sd,
             exclusion = exclusion_criteria)
  curs.execute(createTable)
  conn.commit()
  print("Done.")
  
  print("    - range summary table li_range_{}... ".format(area)),
  createTable = '''
  DROP TABLE IF EXISTS li_range_{area} ; 
  CREATE TABLE li_range_{area} AS
  SELECT p.{area_code},
    {indicators}     
    FROM parcel_indicators p
    LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    {exclusion}
    GROUP BY p.{area_code}
    ORDER BY p.{area_code} ASC;
  ALTER TABLE li_range_{area} ADD PRIMARY KEY ({area_code});
  '''.format(area = area,
             area_code = area_code,
             indicators = ind_range,
             exclusion = exclusion_criteria)
  curs.execute(createTable)
  conn.commit()
  print("Done.")
  
  print("    - median summary table li_median_{}... ".format(area)),  
  createTable = '''
  DROP TABLE IF EXISTS li_median_{area} ; 
  CREATE TABLE li_median_{area} AS
  SELECT p.{area_code},
    {indicators}     
    FROM parcel_indicators p
    LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    {exclusion}
    GROUP BY p.{area_code}
    ORDER BY p.{area_code} ASC;
  ALTER TABLE li_median_{area} ADD PRIMARY KEY ({area_code});
  '''.format(area = area,
             area_code = area_code,
             indicators = ind_median,
             exclusion = exclusion_criteria)
  curs.execute(createTable)
  conn.commit()
  print("Done.")
  
  print("    - IQR summary table li_iqr_{}... ".format(area)),  
  createTable = '''
  DROP TABLE IF EXISTS li_iqr_{area} ; 
  CREATE TABLE li_iqr_{area} AS
  SELECT p.{area_code},
    {indicators}     
    FROM parcel_indicators p
    LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid 
    {exclusion}
    GROUP BY p.{area_code}
    ORDER BY p.{area_code} ASC;
  ALTER TABLE li_iqr_{area} ADD PRIMARY KEY ({area_code});
  '''.format(area = area,
             area_code = area_code,
             indicators = ind_iqr,
             exclusion = exclusion_criteria)
  curs.execute(createTable)
  conn.commit()
  print("Done.")
  
  map_ind_percentile_area = ''
  # if area != 'study_region':
  print("    - percentile summary table li_percentiles_{}... ".format(area)),      
  createTable = '''
    DROP TABLE IF EXISTS li_percentiles_{area} ; 
    CREATE TABLE li_percentiles_{area} AS
    SELECT {area_code},
           {indicators}
    FROM li_inds_{area}_dwelling
    ORDER BY {area_code} ASC;
  ALTER TABLE li_percentiles_{area} ADD PRIMARY KEY ({area_code});
  '''.format(area = area,
             area_code = area_code,
             indicators = ind_percentile)
  curs.execute(createTable)
  conn.commit()
  print("Done.")
  map_ind_percentile_area = '\n{},'.format(map_ind_percentile)
    
  if area_code != 'mb_code_2016':
    # Create shape files for interactive map visualisation
    area_strings   = {'sa1_maincode_2016' :'''area.sa1_maincode_2016 AS sa1   ,\n area.suburb ,\n area.lga ,area.sample_count, area.dwelling, area.person \n''',
                      'ssc_name_2016':''' '-'::varchar AS sa1 ,\n area.ssc_name_2016 AS suburb,\n area.lga AS lga , area.sample_count, area.dwelling, area.person \n''',
                      'lga_name_2016':''' '-'::varchar AS sa1 ,\n '-'::varchar AS suburb ,\n area.lga_name_2016  AS lga ,area.sample_count, area.dwelling, area.person \n''',
                      'sos_name_2016':''' '-'::varchar AS sa1 ,\n '-'::varchar AS suburb ,\n '-'  AS lga , area.sos_name_2016 AS sos \n''',
                      'study_region':'''area.study_region'''
                      }      

    area_tables = {'sa1_maincode_2016' :'''(SELECT a.sa1_maincode_2016, a.sample_count, a.person, a.dwelling, a.geom, string_agg(DISTINCT(l.ssc_name_2016),', ') AS suburb, string_agg(DISTINCT(l.lga_name_2016),', ') AS lga FROM li_inds_sa1_dwelling a LEFT JOIN mb_dwellings l USING (sa1_maincode_2016) GROUP BY a.sa1_maincode_2016,a.sample_count, a.person, a.dwelling,a.geom)''',
                   'ssc_name_2016':'''(SELECT a.ssc_name_2016, a.sample_count, a.person, a.dwelling, a.geom, string_agg(DISTINCT(lga_name_2016),', ') AS lga FROM li_inds_ssc_dwelling a LEFT JOIN area_linkage l USING (ssc_name_2016) GROUP BY a.ssc_name_2016,a.sample_count, a.person, a.dwelling,a.geom)''',
                   'lga_name_2016':'''(SELECT a.lga_name_2016, a.sample_count, a.person, a.dwelling, a.geom, string_agg(DISTINCT(ssc_name_2016),', ') AS suburb FROM li_inds_lga_dwelling a LEFT JOIN area_linkage l USING (lga_name_2016) GROUP BY a.lga_name_2016,a.sample_count, a.person, a.dwelling,a.geom)''',
                   'sos_name_2016': 'study_region_all_sos',
                   'study_region': ''' (SELECT study_region,locale, ST_Union(geom) AS geom FROM area_indicators_mb_json GROUP BY study_region,locale) '''}   
                  
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
      LEFT JOIN li_percentiles_{area} AS perc 
             ON area.{area_code} = perc.{area_code}
      '''.format(area = area,area_code = area_code)

    # Note -i've excerpted SD and median out of the below table for now; too much for SA1s  
    #        {sd},
    #        {median},
    # LEFT JOIN li_median_{area} AS median ON area.{area_code2} = median.{area_code}
    # LEFT JOIN li_sd_{area} AS sd ON area.{area_code2} = raw.{area_code}
    createTable = '''DROP TABLE IF EXISTS li_map_{area}_{locale}_{year};
    CREATE TABLE li_map_{area}_{locale}_{year} AS
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
    LEFT JOIN li_range_{area} AS range ON area.{area_code} = range.{area_code}
    LEFT JOIN li_iqr_{area} AS iqr ON area.{area_code} = iqr.{area_code}
    {area_code_table};
    ALTER TABLE li_map_{area}_{locale}_{year} ADD PRIMARY KEY ({primary_key});
    '''.format(area = area,
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
    curs.execute(createTable)
    conn.commit()
    
    createTable = '''
    DROP TABLE IF EXISTS boundaries_{area}_{locale}_{year};
    CREATE TABLE boundaries_{area}_{locale}_{year} AS
    SELECT b.{area_code},
           ST_Transform(ST_Simplify(b.geom,.1),4326) AS geom         
    FROM {boundaries};
    '''.format(area = area,
               locale = locale,
               year = year,
               area_code = area_code,
               boundaries = boundary_tables[area_code])
    print("    - boundary overlays at {} level".format(area)),
    curs.execute(createTable)
    conn.commit()
    print("Done.")
    
    createTable = '''
    DROP TABLE IF EXISTS urban_sos_{area}_{locale}_{year};
    CREATE TABLE urban_sos_{area}_{locale}_{year} AS
    SELECT urban,
           ST_Transform(ST_Simplify(geom,.1),4326) AS geom         
    FROM study_region_urban
    WHERE urban  = 'urban';
    '''.format(area = area,
               locale = locale,
               year = year,
               area_code = area_code,
               boundaries = boundary_tables[area_code])
    print("    - urban overlays at {} level".format(area)),
    curs.execute(createTable)
    conn.commit()
    print("Done.")

# need to add in a geometry column to ind_observatory to allow for importing of this table as a layer in geoserver
# If it doesn't already exists
# So, check if it already exists
curs.execute("SELECT column_name FROM information_schema.columns WHERE table_name='ind_observatory_{}_{}' and column_name='geom';".format(locale,year))
null_geom_check = curs.fetchall()
if len(null_geom_check)==0:
  # if geom doesn't exist, created it
  curs.execute("SELECT AddGeometryColumn ('public','ind_observatory_{}_{}','geom',4326,'POINT',2);".format(locale,year))
  conn.commit()



map_tables = ["boundaries_sos","boundaries_sa1","boundaries_ssc","boundaries_lga","li_map_region","li_map_sa1","li_map_ssc","li_map_lga","ind_observatory","boundaries_region","boundaries_sa1","boundaries_ssc","boundaries_lga"]

output_tables = ' '.join([' -t "{x}_{locale}_{year}"'.format(x = x,locale = locale,year = year) for x in map_tables])
output_tables_gpkg = ' '.join(['"{x}_{locale}_{year}"'.format(x = x,locale = locale,year = year) for x in map_tables])

print("Output to geopackage gpkg: {path}/li_map_{db}.gpkg".format(path = map_features_outpath, db = db)),

gpkg =  '{}/li_map_{}.gpkg'.format(map_features_outpath,db)

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
           'pg_dump {tables} postgresql://{user}:{pwd}@{host}:5432/{db} '
           '> '
           '{dir}/li_map_{db}.sql'
           ).format(user = db_user,
                    pwd = db_pwd,
                    host = db_host,
                    dir = locale_dir,
                    db = db,
                    tables = output_tables)
sp.call(command, shell=True)
print('''Done; but if it actually did not work, please run the following command:\n
pg_dump -U postgres -h localhost -W  {tables} {db} > {dir}/li_map_{db}.sql
'''.format(locale = locale.lower(), year = year,db = db,tables = output_tables,dir = locale_dir))

print('''
\nAlso, can you send the following line of text to Carl please to aid collation of study regions?
psql obs_source < {dir}/li_map_{db}.sql postgres
'''.format(dir = locale_dir,db = db))