# Script:  area_linkage_tables.py
# Purpose: Create ABS and non-ABS linkage tables using 2016 data sourced from ABS
#
#          Parcel address points are already associated with Meshblock in the parcel_dwellings table
#          Further linkage with the abs_linkage table (actually, a reduced version of the existing mb_dwellings)
#          facilitates aggregation to ABS area units such as SA1, SA2, SA3, SA4.
#
#          The non-ABS linkage table associated points with the suburb and LGA in which they are located, according
#          to ABS sourced spatial features.
#
#          Regarding linkage of addresses with non-ABS structures, while the ABS provides some 
#          correspondence tables between areas, (e.g. SA2 2016 to LGA 2016) this will not be as accurate
#          for our purposes as taking an address point location and evaluating the polygon it intersects.
#          There are pitfalls in this approach (e.g. if a point lies exactly on a boundary), however
#          this is par for the course when generalising unique units into aggregate categories 
#          (ie. points to averages, sums or variances within contiguous areas).
# 
# Author:  Carl Higgs
# Date:    20180710

# Import arcpy module

import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import numpy
import time
import psycopg2 
from progressor import progressor
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *


# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create ABS and non-ABS linkage tables using 2016 data sourced from ABS'

# INPUT PARAMETERS
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db))

# OUTPUT PROCESS
task = 'Create ABS and non-ABS linkage tables using 2016 data sourced from ABS'
print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))
# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()


create_abslinkage_Table     = '''
  DROP TABLE IF EXISTS abs_linkage;
  CREATE TABLE IF NOT EXISTS abs_linkage AS
    SELECT 
      mb_code_2016                    ,
      mb_category_name_2016           ,
      dwelling                        ,
      person                          ,                 
      sa1_mainco AS sa1_maincode      ,
      sa2_name_2 AS sa2_name_2016     ,
      sa3_name_2 AS sa3_name_2016     ,
      sa4_name_2 AS sa4_name_2016     ,
      gccsa_name                      ,
      state_name                      ,
      area_albers_sqkm                ,
      shape_area/10000 AS area_ha     ,
      geom
    FROM 
      mb_dwellings ;
  ALTER  TABLE abs_linkage ADD PRIMARY KEY (mb_code_2016);
  CREATE INDEX IF NOT EXISTS mb_code_2016_gix ON abs_linkage USING GIST (geom);
  '''

curs.execute(create_abslinkage_Table)
conn.commit()

for area in areas:
  print('{}: '.format(areas[area]['name_f'])), 
  command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" -a_srs "EPSG:{srid}" '.format(srid = srid) \
          + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
          + 'user={user} password = {pwd}" '.format(user = db_user,pwd = db_pwd) \
          + '{shp} '.format(shp = areas[area]['data']) \
          + '-lco geometry_name="geom"  -lco precision=NO ' \
          + '-nlt MULTIPOLYGON' 
  print(command)
  sp.call(command, shell=True) 
  curs.execute('''
  DELETE FROM  {area} a 
        USING {buffered_study_region} b 
    WHERE NOT ST_Intersects(a.geom,b.geom) 
           OR a.geom IS NULL;
  '''.format(area = areas[area]['table'],
             buffered_study_region = buffered_study_region))
  conn.commit()
  
print("Granting privileges to python and arcgis users... "),
curs.execute(grant_query)
conn.commit()
print("Done.")

create_non_abslinkage_Table     = '''
  DROP TABLE IF EXISTS non_abs_linkage;
  CREATE TABLE IF NOT EXISTS non_abs_linkage AS
    SELECT
      {0},
      ssc_code_2 AS ssc_code_2016,
      ssc_name_2 AS ssc_name_2016,
      lga_code_2 AS lga_code_2016,
      lga_name_2 AS lga_name_2016
      from parcel_dwellings a, 
      {1} b, 
      {2} c 
      where st_contains(b.geom,a.geom) AND st_contains(c.geom,a.geom);
  ALTER  TABLE non_abs_linkage ADD PRIMARY KEY ({0});
  '''.format(points_id,suburb_feature, lga_feature)


print("Create non-ABS linkage table (linking point IDs with suburbs and LGAs... "),
curs.execute(create_non_abslinkage_Table)
conn.commit()
print("Done")
  
print("Import area level disadvantage data... "),
disadvantage = pandas.read_csv(area_info['disadvantage']['data'], index_col = area_info['disadvantage']['id'])
disadvantage.index = disadvantage.index.map(str)
region_limit = '''
SELECT {id} 
  FROM {table},{buffered_study_region}
 WHERE ST_Intersects({table}.geom,{buffered_study_region}.geom)
 '''.format(id = areas[area_info['disadvantage']['area']]['id'],
            table = os.path.splitext(os.path.basename(areas[area_info['disadvantage']['area']]['data']))[0].lower(),
            buffered_study_region = buffered_study_region)
areas_in_region = pandas.read_sql_query(region_limit,con=engine,index_col=areas[area_info['disadvantage']['area']]['id'])
disadvantage = areas_in_region.merge(disadvantage,how='left', left_index=True, right_index=True)
disadvantage.to_sql(area_info['disadvantage']['table'], index_label =area_info['disadvantage']['id'],con = engine, if_exists='replace')
print("Done.")


# Create study region tables
print("  - Study region tables (urban, not urban, all sos; within study region bounds)")
# Create study region tables
create_study_region_tables = '''
  DROP TABLE IF EXISTS study_region_all_sos;
  CREATE TABLE study_region_all_sos AS 
  SELECT b.sos_name_2 AS sos_name_2016, 
         CASE 
            WHEN ST_CoveredBy(a.geom, b.geom) 
                THEN b.geom 
            ELSE 
                ST_CollectionExtract(ST_Multi(
                    ST_Intersection(a.geom, b.geom)
                    ),3) END AS geom
    FROM {region}_{year} a
    INNER JOIN main_sos_2016_aust b 
    ON (ST_Intersects(a.geom,b.geom));
  CREATE INDEX IF NOT EXISTS study_region_all_sos_idx ON study_region_all_sos USING GIST (geom);
  
  DROP TABLE IF EXISTS study_region_urban;
  CREATE TABLE study_region_urban AS 
  SELECT * 
    FROM study_region_all_sos
   WHERE sos_name_2016 IN ('Major Urban', 'Other Urban');
  CREATE INDEX IF NOT EXISTS study_region_urban_idx ON study_region_urban USING GIST (geom);
  
  DROP TABLE IF EXISTS study_region_not_urban;
  CREATE TABLE study_region_not_urban AS 
  SELECT * 
    FROM study_region_all_sos
   WHERE sos_name_2016 NOT IN ('Major Urban', 'Other Urban');
  CREATE INDEX IF NOT EXISTS study_region_not_urban_idx ON study_region_not_urban USING GIST (geom);

  DROP TABLE IF EXISTS study_region_ssc;
  CREATE TABLE study_region_ssc AS 
  SELECT b.ssc_name_2 AS ssc_name_2016, 
         b.geom
    FROM {region}_{year} a, 
         main_ssc_2016_aust b 
   WHERE ST_Intersects(a.geom,b.geom);  
  CREATE INDEX IF NOT EXISTS study_region_ssc_idx ON study_region_ssc USING GIST (geom);
'''.format(region = region.lower(), year = year)
curs.execute(create_study_region_tables)
conn.commit()

print("  - SOS indexed by parcel")
create_parcel_sos = '''
  DROP TABLE IF EXISTS parcel_sos;
  CREATE TABLE parcel_sos AS 
  SELECT a.{id},
         sos_name_2016 
  FROM parcel_dwellings a,
       study_region_all_sos b 
  WHERE ST_Intersects(a.geom,b.geom);
  CREATE UNIQUE INDEX IF NOT EXISTS parcel_sos_idx ON  parcel_sos (gnaf_pid);
  '''.format(id = points_id)
curs.execute(create_parcel_sos)
conn.commit()

# create excluded Mesh Block table
create_mb_excluded_no_irsd = '''  
  DROP TABLE IF EXISTS mb_excluded_no_irsd;
  CREATE TABLE mb_excluded_no_irsd AS
  SELECT * FROM abs_linkage 
  WHERE sa1_maincode NOT IN (SELECT sa1_maincode FROM area_disadvantage);
  '''
print("  - Meshblocks, excluded due to no IRSD")
curs.execute(create_mb_excluded_no_irsd)
conn.commit()

# create excluded Mesh Block table
create_mb_no_dwellings = '''  
  DROP TABLE IF EXISTS mb_no_dwellings;
  CREATE TABLE mb_no_dwellings AS
  SELECT meshblocks.* FROM meshblocks, {study_region}
  WHERE mb_code_20 NOT IN (SELECT mb_code_2016 FROM mb_dwellings)
  AND ST_Intersects(meshblocks.geom,{study_region}.geom);
  '''.format(study_region = study_region)
print("  - Meshblocks, excluded due to no dwellings")
curs.execute(create_mb_no_dwellings)
conn.commit()

# create excluded Mesh Block table
create_area_no_irsd = '''  
  DROP TABLE IF EXISTS area_no_irsd;
  CREATE TABLE area_no_irsd AS
  SELECT  'Meshblocks in SA1s without SEIFA IRSD (2016)'::varchar AS description, 
          ST_Union(geom) AS geom FROM mb_excluded_no_irsd;
  '''
print("  - Total area excluded due to no IRSD")
curs.execute(create_area_no_irsd)
conn.commit()

# create excluded Mesh Block table
create_area_no_dwelling = '''  
  DROP TABLE IF EXISTS area_no_dwelling;
  CREATE TABLE area_no_dwelling AS
  SELECT 'Meshblocks with no dwellings (2016)'::varchar AS description,
          ST_Union(geom) AS geom  FROM mb_no_dwellings;
  '''
 print("  - Total area excluded due to no dwellings")
curs.execute(create_area_no_dwelling)
conn.commit()
print("Done.")
 
create_no_sausage_sos_tally = '''
  DROP TABLE IF EXISTS no_sausage_sos_tally;
  CREATE TABLE no_sausage_sos_tally AS
  SELECT sos_name_2 AS section_of_state, 
         count(b.*) AS no_sausage_count,
         count(b.*) / (SELECT COUNT(*) FROM parcel_dwellings)::double precision AS no_sausage_prop
  FROM main_sos_2016_aust a 
  LEFT JOIN no_sausage b ON ST_Intersects(a.geom,b.geom) 
  GROUP BY section_of_state 
  ORDER BY no_sausage_count DESC;
  DELETE FROM no_sausage_sos_tally WHERE no_sausage_count = 0;
'''
print("Make a summary table of parcel points lacking sausage buffer, grouped by section of state (the idea is, only a small proportion should be major or other urban"),
curs.execute(create_no_sausage_sos_tally)
conn.commit()
print("Done.")

createTable_nh1600m = '''
  DROP TABLE IF EXISTS nh1600m;
  CREATE TABLE IF NOT EXISTS nh1600m AS
    SELECT {0}, area_sqm, area_sqm/1000000 AS area_sqkm, area_sqm/10000 AS area_ha FROM 
      (SELECT {0}, ST_AREA(geom) AS area_sqm FROM {1}) AS t;
  ALTER TABLE nh1600m ADD PRIMARY KEY ({0});
  '''.format(points_id.lower(),"sausagebuffer_{}".format(distance))

print("Creating summary table of parcel id and area... "),
curs.execute(createTable_nh1600m)
conn.commit()  
print("Done.")

# output to completion log    
script_running_log(script, task, start, locale)

# clean up
conn.close()
   