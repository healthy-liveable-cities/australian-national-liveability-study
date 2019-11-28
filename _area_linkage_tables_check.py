# Script:  _area_linkage_tables_check.py
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
from _project_setup import *


# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create ABS and non-ABS linkage tables using 2016 data sourced from ABS'

# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

# INPUT PARAMETERS
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db))

if 'SUA' in region_shape:
    print("Re-import significant urban areas feature...")
    sua = os.path.splitext(os.path.basename(region_shape))[0].lower()
    command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" -a_srs "EPSG:{srid}" '.format(srid = srid) \
              + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
              + 'user={user} password = {pwd}" '.format(user = db_user,pwd = db_pwd) \
              + '{shp} '.format(shp = region_shape) \
              + '-lco geometry_name="geom"  -lco precision=NO ' \
              + '-nlt MULTIPOLYGON' 
    # print(command)
    sp.call(command, shell=True) 
    curs.execute('''
    DROP TABLE IF EXISTS {study_region}_wrong_20190618;
    DROP TABLE IF EXISTS {buffered_study_region}_wrong_20190618;
    ALTER TABLE IF EXISTS {study_region} RENAME TO {study_region}_wrong_20190618;
    ALTER TABLE IF EXISTS {buffered_study_region} RENAME TO {buffered_study_region}_wrong_20190618;
    CREATE TABLE {study_region} AS
        SELECT * 
        FROM {area}
        WHERE {where_clause};
    CREATE TABLE {buffered_study_region} AS
        SELECT ST_Buffer(geom,10000) AS geom
        FROM {study_region};
    DELETE FROM  {area} a 
          USING {buffered_study_region} b 
      WHERE NOT ST_Intersects(a.geom,b.geom) 
             OR a.geom IS NULL;
    CREATE INDEX IF NOT EXISTS {study_region}_idx ON {study_region} USING GIST (geom);
    CREATE INDEX IF NOT EXISTS {buffered_study_region}_idx ON {buffered_study_region} USING GIST (geom);
    '''.format(area = sua ,
               study_region = study_region,
               buffered_study_region = buffered_study_region,
               where_clause = region_where_clause.replace('SUA_NAME_2','sua_name_2')))
    conn.commit()

print("Re-import sections of state feature...")
sos = areas['urban']
command = 'ogr2ogr -overwrite -progress -f "PostgreSQL" -a_srs "EPSG:{srid}" '.format(srid = srid) \
          + 'PG:"host={host} port=5432 dbname={db} '.format(host = db_host,db = db) \
          + 'user={user} password = {pwd}" '.format(user = db_user,pwd = db_pwd) \
          + '{shp} '.format(shp = sos['data']) \
          + '-lco geometry_name="geom"  -lco precision=NO ' \
          + '-nlt MULTIPOLYGON' 
# print(command)
sp.call(command, shell=True) 

task = 'Create ABS and non-ABS linkage tables using 2016 data sourced from ABS'
print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))

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

curs.execute(grant_query)
conn.commit()

# output to completion log    
script_running_log(script, task, start, locale)

# clean up
conn.close()
   