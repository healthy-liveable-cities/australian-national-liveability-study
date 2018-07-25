# Script:  19_area_indicators.py
# Purpose: Create area level indicator tables
# Author:  Carl Higgs 
# Date:    20 July 2018


#### Sketch!! 

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

# Create query for indicator range (including scaling of percent variables)
ind_range = ',\n'.join("ROUND(MIN(" +
                       ind_matrix['agg_scale'].apply(lambda x: '100*' if x == 100 else '') +
                       ind_matrix['indicators'] + 
                       ")::numeric,1)::text || ' - ' ||ROUND(MAX(" +
                       ind_matrix['agg_scale'].apply(lambda x: '100*' if x == 100 else '') +
                       ind_matrix['indicators'] + 
                       ")::numeric,1)::text AS " +
                       ind_matrix['indicators'])

# Create query for Interquartile range interval (25% to 75%) to represent the range within which the middle 50% of observations lie                       
ind_iqr = ',\n'.join("round(percentile_cont(0.25) WITHIN GROUP (ORDER BY " +
                       ind_matrix['agg_scale'].apply(lambda x: '100*' if x == 100 else '') +
                       ind_matrix['indicators'] + 
                       ")::numeric,1)::text || ' - ' ||round(percentile_cont(0.75) WITHIN GROUP (ORDER BY " +
                       ind_matrix['agg_scale'].apply(lambda x: '100*' if x == 100 else '') +
                       ind_matrix['indicators'] + 
                       ")::numeric,1)::text AS " +
                       ind_matrix['indicators'])                  

# Create a second pass table including binary indicators
## TO DO

# Create query for percentile           
ind_percentile = ',\n'.join("round(100*cume_dist() OVER(ORDER BY "+
                            ind_matrix['indicators'] + 
                            ")::numeric,0) AS " +
                            ind_matrix['indicators'])        

# Map query for raw indicators
map_ind_raw = ',\n'.join("round(raw." +
                         ind_matrix['indicators'] + 
                         "::numeric,1) AS r_" +
                         ind_matrix['indicators'])                          
 
# Map query for percentile indicators
map_ind_percentile = ',\n'.join("round(perc." +
                          ind_matrix['indicators'] + 
                          "::numeric,1) AS p_" +
                          ind_matrix['indicators'])               
 
# Map query for range indicators
map_ind_range = ',\n'.join("range." + 
                           ind_matrix['indicators'] + 
                           " AS d_" +
                           ind_matrix['indicators'])                   
 
# Map query for iqr indicators
map_ind_iqr = ',\n'.join("iqr." + 
                         ind_matrix['indicators'] + 
                         " AS m_" +
                         ind_matrix['indicators']) 
                         
exclusion_criteria = 'WHERE  {0} NOT IN (SELECT DISTINCT({0}) FROM excluded_parcels)'.format(points_id.lower())
## I'm not sure if the below is still relevant
# parcelmb_exclusion_criteria = 'WHERE  parcelmb.{0} NOT IN (SELECT DISTINCT({0}) FROM excluded_parcels)'.format(points_id.lower())



# The shape file for map features are output 
map_features_outpath = os.path.join(folderPath,'study_region','wgs84_epsg4326','map_features')

if not os.path.exists(map_features_outpath):
  os.makedirs(map_features_outpath)   

      
      
# SQL Settings
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

areas = {'mb_code_2016':'mb',
         'sa1_maincode':'sa1',
         'ssc_name_2016':'ssc',
         'lga_name_2016':'lga'}
  
# create aggregated raw liveability estimates for selected area
for area_code in areas.keys():
  area = areas[area_code]
  print("Create aggregate indicator table li_inds_{}... ".format(area)),
  createTable = '''
  DROP TABLE IF EXISTS li_inds_{area} ; 
  CREATE TABLE li_inds_{area} AS
  SELECT {area_code},
    {indicators}
    FROM parcel_indicators
    {exclusion}
    GROUP BY {area_code}
    ORDER BY {area_code} ASC;
  ALTER TABLE li_inds_{area} ADD PRIMARY KEY ({area_code});
  '''.format(area = area,
             area_code = area_code,
             indicators = ind_avg,
             exclusion = exclusion_criteria)
  curs.execute(createTable)
  conn.commit()
  print("Done.")
  
  
  ### Note: for now, we are just doing the continuous scale indicators with averages; later I'll implement a second pass to evaluate the threshold cutoffs.
  
  print("Create range summary table li_range_{}... ".format(area)),
  createTable = '''
  DROP TABLE IF EXISTS li_range_{area} ; 
  CREATE TABLE li_range_{area} AS
  SELECT {area_code},
    {indicators}     
    FROM parcel_indicators
    {exclusion}
    GROUP BY {area_code}
    ORDER BY {area_code} ASC;
  ALTER TABLE li_range_{area} ADD PRIMARY KEY ({area_code});
  '''.format(area = area,
             area_code = area_code,
             indicators = ind_range,
             exclusion = exclusion_criteria)
  curs.execute(createTable)
  conn.commit()
  print("Done.")
  
  print("Create IQR summary table li_iqr_{}... ".format(area)),  
  createTable = '''
  DROP TABLE IF EXISTS li_iqr_{area} ; 
  CREATE TABLE li_iqr_{area} AS
  SELECT {area_code},
    {indicators}     
    FROM parcel_indicators
    {exclusion}
    GROUP BY {area_code}
    ORDER BY {area_code} ASC;
  ALTER TABLE li_iqr_{area} ADD PRIMARY KEY ({area_code});
  '''.format(area = area,
             area_code = area_code,
             indicators = ind_iqr,
             exclusion = exclusion_criteria)
  curs.execute(createTable)
  conn.commit()
  print("Done.")
  
  print("Create percentile summary table li_percentiles_{}... ".format(area)),      
  createTable = '''
  DROP TABLE IF EXISTS li_percentiles_{area} ; 
  CREATE TABLE li_percentiles_{area} AS
  SELECT {area_code},
         {indicators}
  FROM li_inds_{area}
  ORDER BY {area_code} ASC;
  ALTER TABLE li_percentiles_{area} ADD PRIMARY KEY ({area_code});
  '''.format(area = area,
             area_code = area_code,
             indicators = ind_percentile)
  curs.execute(createTable)
  conn.commit()
  print("Done.")
  
  if area_code != 'mb_code_2016':
    # Create shape files for interactive map visualisation
    area_strings   = {'sa1_maincode' :'''area.sa1_maincode AS sa1   ,\n area.suburb ,\n area.lga ,\n''',
                      'ssc_name_2016':''' '-'::varchar AS sa1 ,\n area.suburb AS suburb,\n area.lga AS lga ,\n''',
                      'lga_name_2016':''' '-'::varchar AS sa1 ,\n '-'::varchar AS suburb ,\n area.lga  AS lga ,\n'''}    
    
    area_code2 = {'sa1_maincode' :'sa1_maincode',
                  'ssc_name_2016':'suburb',
                  'lga_name_2016':'lga'}    
    
    area_code_tables = {'sa1_maincode' :'''LEFT JOIN main_sa1_2016_aust_full AS area_code ON area.sa1_maincode = area_code.sa1_mainco''',
                        'ssc_name_2016':'''LEFT JOIN main_ssc_2016_aust      AS area_code ON area.suburb       = area_code.ssc_name_2''',
                        'lga_name_2016':'''LEFT JOIN main_lga_2016_aust      AS area_code ON area.lga          = area_code.lga_name_2'''}
                      
    community_code = {'sa1_maincode' :'''area_code.sa1_7digit AS community_code''',
                      'ssc_name_2016':'''CONCAT('SSC',area_code.ssc_code_2::varchar) AS community_code''',
                      'lga_name_2016':'''CONCAT('LGA',area_code.lga_code_2::varchar) AS community_code'''}
                    
    createTable = '''DROP TABLE IF EXISTS li_map_{area};
    CREATE TABLE li_map_{area} AS
    SELECT {area_strings}
           area.resid_parcels,
           area.dwellings,
           area.resid_persons,
           {raw},
           {percentile},
           {range},
           {iqr},
           {community_code},
           ST_TRANSFORM(area.geom,4326) AS geom              
    FROM area_{area} AS area
    LEFT JOIN li_inds_{area} AS raw ON area.{area_code2} = raw.{area_code}
    LEFT JOIN li_percentiles_{area} AS perc ON area.{area_code2} = perc.{area_code}
    LEFT JOIN li_range_{area} AS range ON area.{area_code2} = range.{area_code}
    LEFT JOIN li_iqr_{area} AS iqr ON area.{area_code2} = iqr.{area_code}
    {area_code_table};
    '''.format(area = area,
               area_code = area_code,
               area_strings = area_strings[area_code],
               raw = map_ind_raw,
               percentile = map_ind_percentile,
               range = map_ind_range,
               iqr = map_ind_iqr,
               community_code = community_code[area_code],
               area_code2 = area_code2[area_code],
               area_code_table = area_code_tables[area_code])
    print("Creating map feature at {} level".format(area))
    curs.execute(createTable)
    conn.commit()

print("Output to geopackage gpkg: {path}/li_map.gpkg... ".format(path = map_features_outpath)),
command = 'ogr2ogr -overwrite -f GPKG {path}/li_map_{db}.gpkg PG:"host={host} user={user} dbname={db} password={pwd}" "li_map_sa1" "li_map_ssc" "li_map_lga"'.format(path = map_features_outpath,
                                                                                                                                                                host = db_host,
                                                                                                                                                                user = db_user,
                                                                                                                                                                pwd = db_pwd,
                                                                                                                                                                db = db)
sp.call(command)
print("Done.")
    
print("Created SA1, suburb and LGA level tables for map web app.")
conn.close()
  
# output to completion log    
script_running_log(script, task, start, locale)