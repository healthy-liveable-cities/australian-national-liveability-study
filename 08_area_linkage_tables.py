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

print("Import region data... "),
for geo in geo_imports.index.values:
  data = geo_imports.loc[geo,'data']
  if os.path.splitext(data)[1]=='.gpkg':
      from_epsg = int(geo_imports.loc[geo,'epsg'])
      command = (
              ' ogr2ogr -overwrite -progress -f "PostgreSQL" '
              ' -s_srs "EPSG:{from_epsg}" -t_srs "EPSG:{to_epsg}" ' 
              ' PG:"host={host} port=5432 dbname={db}'
              ' user={user} password = {pwd}" '
              ' "{gpkg}" '
              ' -lco geometry_name="geom"'.format(host = db_host,
                                           db = db,
                                           user = db_user,
                                           pwd = db_pwd,
                                           from_epsg = from_epsg,
                                           to_epsg = srid,
                                           gpkg = data) 
              )
      print(command)
      sp.call(command, shell=True,cwd=os.path.dirname(os.path.join(folderPath,clean_intersections_gpkg)))

print("Done.")

print("Create study region... "),
sql = '''
CREATE TABLE IF NOT EXISTS study_region AS 
SELECT ST_Union(geom) AS geom
FROM {} 
WHERE {}
'''.format(region_shape,region_where_clause)
curs.execute(sql)
conn.commit()
print("Done.")

print("Create buffered study region... "),
sql = '''
CREATE TABLE IF NOT EXISTS buffered_study_region AS 
SELECT ST_Buffer(geom,{}) AS geom 
FROM study_region
'''.format(study_buffer)
curs.execute(sql)
conn.commit()
print("Done.")

print("Remove from region tables records whose geometries do not intersect buffered study region bounds ... ")
for area in df_regions.table.dropna().values:
    print(" - {}".format(area))
    curs.execute('''
    DELETE FROM  {area} a 
          USING {buffered_study_region} b 
      WHERE NOT ST_Intersects(a.geom,b.geom) 
             OR a.geom IS NULL;
    '''.format(area = area,
               buffered_study_region = buffered_study_region))
    conn.commit()

print("Create buffered study region... "),
sql = '''
CREATE TABLE IF NOT EXISTS buffered_study_region AS 
SELECT ST_Buffer(geom,{}) AS geom 
FROM study_region
'''.format(study_buffer)
curs.execute(sql)
conn.commit()
print("Done.")

print("Initiate area linkage table based on smallest region in region list (first entry: {})... )".format(geographies[0])),

area_linkage = pandas.read_sql_table(df_regions.loc[geographies[0],'table'],con=engine,index_col=df_regions.loc[geographies[0],'id']).reset_index()

# drop the geom column
area_linkage = area_linkage.loc[:,[x for x in area_linkage.columns if x not in ['geom']]]

print("\nMerging csv source data with area linkage table:")
for csv in csv_linkage:
    print('\n{}'.format(csv))
    data_list  = df_regions.loc[csv,'data'].split(',')
    print('  - {}'.format(data_list))
    linkage_id = df_regions.loc[csv,'linkage_id']
    retain = (','.join(df_regions.loc[csv][['linkage_id','linkage_retain']].dropna().values.tolist())).split(',')
    print('  - {}'.format(retain))
    if len(data_list) > 1:
        dfs = [pandas.read_csv(f, 
                                   compression='infer', 
                                   header=0, 
                                   sep=',', 
                                   quotechar='"') 
                    for f in data_list]
        df = pandas.concat(dfs).sort_index()
    else:
        df = pandas.read_csv(data_list[0], 
                                   compression='infer', 
                                   header=0, 
                                   sep=',', 
                                   quotechar='"') 
    df.columns = map(str.lower, df.columns)
    df = df.loc[:,retain].reset_index()
    df[linkage_id] = df[linkage_id].astype(str) 
    area_linkage = pandas.merge(area_linkage,df, on=linkage_id, how = 'left',left_index=True)

# remove redundant merged indices
area_linkage = area_linkage.loc[:,[x for x in area_linkage.columns if x not in ['index_x','index_y']]]

# set geographies[0] code as index
area_linkage = area_linkage.set_index(df_regions.loc[geographies[0],'id'])
    
area_linkage.to_sql('area_linkage',con = engine, if_exists='replace')
print("Done.")

print("\nRemove area records where dwellings are zero and recreate area linkage table geometries and GIST index... "),
# Hacky work around since geopandas isn't necessarily installed on our computers
sql = '''
DELETE FROM area_linkage WHERE dwelling = 0;
ALTER TABLE area_linkage ADD COLUMN geom geometry;
UPDATE area_linkage a 
   SET geom = g.geom
  FROM {table} g
 WHERE a.{id} = g.{id};
CREATE INDEX area_linkage_gidx ON area_linkage USING GIST (geom);
'''.format(table = df_regions.loc[geographies[0],'table'],
           id    = df_regions.loc[geographies[0],'id'])
curs.execute(sql)
conn.commit()
print("Done.")
  
print("Granting privileges to python and arcgis users... "),
curs.execute(grant_query)
conn.commit()
print("Done.")

# Create study region tables
# create_study_region_tables = '''
  # DROP TABLE IF EXISTS study_region_all_sos;
  # CREATE TABLE study_region_all_sos AS 
  # SELECT b.sos_name_2 AS sos_name_2016, 
         # CASE 
            # WHEN ST_CoveredBy(a.geom, b.geom) 
                # THEN b.geom 
            # ELSE 
                # ST_CollectionExtract(ST_Multi(
                    # ST_Intersection(a.geom, b.geom)
                    # ),3) END AS geom
    # FROM {region}_{year} a
    # INNER JOIN main_sos_2016_aust b 
    # ON (ST_Intersects(a.geom,b.geom));
  # CREATE INDEX IF NOT EXISTS study_region_all_sos_idx ON study_region_all_sos USING GIST (geom);
  
  # DROP TABLE IF EXISTS study_region_urban;
  # CREATE TABLE study_region_urban AS 
  # SELECT * 
    # FROM study_region_all_sos
   # WHERE sos_name_2016 IN ('Major Urban', 'Other Urban');
  # CREATE INDEX IF NOT EXISTS study_region_urban_idx ON study_region_urban USING GIST (geom);
  
  # DROP TABLE IF EXISTS study_region_not_urban;
  # CREATE TABLE study_region_not_urban AS 
  # SELECT * 
    # FROM study_region_all_sos
   # WHERE sos_name_2016 NOT IN ('Major Urban', 'Other Urban');
  # CREATE INDEX IF NOT EXISTS study_region_not_urban_idx ON study_region_not_urban USING GIST (geom);

  # DROP TABLE IF EXISTS study_region_ssc;
  # CREATE TABLE study_region_ssc AS 
  # SELECT b.ssc_name_2 AS ssc_name_2016, 
         # b.geom
    # FROM {region}_{year} a, 
         # main_ssc_2016_aust b 
   # WHERE ST_Intersects(a.geom,b.geom);  
  # CREATE INDEX IF NOT EXISTS study_region_ssc_idx ON study_region_ssc USING GIST (geom);
# '''.format(region = region.lower(), year = year)
# curs.execute(create_study_region_tables)
# conn.commit()

# print("  - SOS indexed by parcel")
# create_parcel_sos = '''
  # DROP TABLE IF EXISTS parcel_sos;
  # CREATE TABLE parcel_sos AS 
  # SELECT a.{id},
         # sos_name_2016 
  # FROM parcel_dwellings a,
       # study_region_all_sos b 
  # WHERE ST_Intersects(a.geom,b.geom);
  # CREATE UNIQUE INDEX IF NOT EXISTS parcel_sos_idx ON  parcel_sos (gnaf_pid);
  # '''.format(id = points_id)
# curs.execute(create_parcel_sos)
# conn.commit()

# # create excluded Mesh Block table
# create_mb_excluded_no_irsd = '''  
  # DROP TABLE IF EXISTS mb_excluded_no_irsd;
  # CREATE TABLE mb_excluded_no_irsd AS
  # SELECT * FROM abs_linkage 
  # WHERE sa1_maincode NOT IN (SELECT sa1_maincode FROM area_disadvantage);
  # '''
# print("  - Meshblocks, excluded due to no IRSD")
# curs.execute(create_mb_excluded_no_irsd)
# conn.commit()

# # create excluded Mesh Block table
# create_mb_no_dwellings = '''  
  # DROP TABLE IF EXISTS mb_no_dwellings;
  # CREATE TABLE mb_no_dwellings AS
  # SELECT meshblocks.* FROM meshblocks, {study_region}
  # WHERE mb_code_20 NOT IN (SELECT mb_code_2016 FROM mb_dwellings)
  # AND ST_Intersects(meshblocks.geom,{study_region}.geom);
  # '''.format(study_region = study_region)
# print("  - Meshblocks, excluded due to no dwellings")
# curs.execute(create_mb_no_dwellings)
# conn.commit()

# # create excluded Mesh Block table
# create_area_no_irsd = '''  
  # DROP TABLE IF EXISTS area_no_irsd;
  # CREATE TABLE area_no_irsd AS
  # SELECT  'Meshblocks in SA1s without SEIFA IRSD (2016)'::varchar AS description, 
          # ST_Union(geom) AS geom FROM mb_excluded_no_irsd;
  # '''
# print("  - Total area excluded due to no IRSD")
# curs.execute(create_area_no_irsd)
# conn.commit()

# # create excluded Mesh Block table
# create_area_no_dwelling = '''  
  # DROP TABLE IF EXISTS area_no_dwelling;
  # CREATE TABLE area_no_dwelling AS
  # SELECT 'Meshblocks with no dwellings (2016)'::varchar AS description,
          # ST_Union(geom) AS geom  FROM mb_no_dwellings;
  # '''
 # print("  - Total area excluded due to no dwellings")
# curs.execute(create_area_no_dwelling)
# conn.commit()
# print("Done.")
 
# create_no_sausage_sos_tally = '''
  # DROP TABLE IF EXISTS no_sausage_sos_tally;
  # CREATE TABLE no_sausage_sos_tally AS
  # SELECT sos_name_2 AS section_of_state, 
         # count(b.*) AS no_sausage_count,
         # count(b.*) / (SELECT COUNT(*) FROM parcel_dwellings)::double precision AS no_sausage_prop
  # FROM main_sos_2016_aust a 
  # LEFT JOIN no_sausage b ON ST_Intersects(a.geom,b.geom) 
  # GROUP BY section_of_state 
  # ORDER BY no_sausage_count DESC;
  # DELETE FROM no_sausage_sos_tally WHERE no_sausage_count = 0;
# '''
# print("Make a summary table of parcel points lacking sausage buffer, grouped by section of state (the idea is, only a small proportion should be major or other urban"),
# curs.execute(create_no_sausage_sos_tally)
# conn.commit()
# print("Done.")

# createTable_nh1600m = '''
  # DROP TABLE IF EXISTS nh1600m;
  # CREATE TABLE IF NOT EXISTS nh1600m AS
    # SELECT {0}, area_sqm, area_sqm/1000000 AS area_sqkm, area_sqm/10000 AS area_ha FROM 
      # (SELECT {0}, ST_AREA(geom) AS area_sqm FROM {1}) AS t;
  # ALTER TABLE nh1600m ADD PRIMARY KEY ({0});
  # '''.format(points_id.lower(),"sausagebuffer_{}".format(distance))

# print("Creating summary table of parcel id and area... "),
# curs.execute(createTable_nh1600m)
# conn.commit()  
# print("Done.")

# # output to completion log    
# script_running_log(script, task, start, locale)

# # clean up
# conn.close()

