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
# importing arcpy as legacy dependency (using resource in gdb for network analysis)
import arcpy

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *


# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = '\nCreate area linkage tables using 2016 data sourced from ABS'

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db))

print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))
# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()

print("\nImport region data... "),
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
print('''(note that a warning "Did not recognize type 'geometry' of column 'geom'" may appear; this is fine.)''')
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
area_linkage = area_linkage.loc[:,[x for x in area_linkage.columns if x not in ['index_x','index_y','index']]]

# set geographies[0] code as index
area_linkage = area_linkage.set_index(df_regions.loc[geographies[0],'id'])
    
area_linkage.to_sql('area_linkage',con = engine, if_exists='replace')
print("Done.")                   

print("\nRemove area records where dwellings are zero and recreate area linkage table geometries and GIST index... "),
# work around since geopandas isn't necessarily installed on our computers
sql = '''
---- commented out dwelling exclusion; this is arguably excessive for the linkage table
-- DELETE FROM area_linkage WHERE dwelling = 0;
ALTER TABLE area_linkage ADD COLUMN urban text;
ALTER TABLE area_linkage ADD COLUMN study_region boolean;
ALTER TABLE area_linkage ADD COLUMN area_ha double precision;
ALTER TABLE area_linkage ADD COLUMN geom geometry;
UPDATE area_linkage a 
   SET urban = CASE                                                       
                   WHEN sos_name_2016 IN ('Major Urban','Other Urban')  
                   THEN 'urban'                                           
                   ELSE 'not urban'                                       
                END, 
       area_ha = ST_Area(g.geom)/10000.0,
       geom = g.geom
  FROM {table} g
 WHERE a.{id} = g.{id};
UPDATE area_linkage a 
   SET study_region = ST_CoveredBy(a.geom,s.geom)
  FROM study_region s;
CREATE INDEX gix_area_linkage ON area_linkage USING GIST (geom);
DROP TABLE IF EXISTS mb_dwellings;
CREATE TABLE mb_dwellings AS 
      SELECT *
        FROM area_linkage
       WHERE dwelling = 0;
CREATE UNIQUE INDEX ix_mb_dwellings ON area_linkage (mb_code_2016);
CREATE INDEX gix_mb_dwellings ON area_linkage USING GIST (geom);
'''.format(table = df_regions.loc[geographies[0],'table'],
           id    = df_regions.loc[geographies[0],'id'])
curs.execute(sql)
conn.commit()
print("Done.")
  
print("Granting privileges to python and arcgis users... "),
curs.execute(grant_query)
conn.commit()
print("Done.")           


## ArcGIS code (currently necessary due to legacy usage in lead up to network analysis)
# ArcGIS environment settings
arcpy.env.workspace = locale_dir  
# create project specific folder in temp dir for scratch.gdb, if not exists
if not os.path.exists(os.path.join(temp,db)):
    os.makedirs(os.path.join(temp,db))
    
arcpy.env.scratchWorkspace = os.path.join(temp,db)  
arcpy.env.overwriteOutput = True 

# define spatial reference
SpatialRef = arcpy.SpatialReference(SpatialRef)

# OUTPUT PROCESS
# Create output gdb if not already existing
if os.path.exists(gdb_path):
  print("Using extant file geodatabase: {}".format(gdb_path)) 
if not os.path.exists(gdb_path):
  arcpy.CreateFileGDB_management(locale_dir,gdb)
  print("File geodatabase created: {}".format(gdb_path))

# copy study region, buffered study region and mb_dwellings to arcgis
arcpy.env.workspace = db_sde_path
arcpy.env.overwriteOutput = True

arcpy.CopyFeatures_management('public.{}'.format(study_region), os.path.join(gdb_path,study_region))
arcpy.CopyFeatures_management('public.{}'.format(buffered_study_region), os.path.join(gdb_path,buffered_study_region))
arcpy.CopyFeatures_management('public.mb_dwellings', os.path.join(gdb_path,'mb_dwellings'))

# output buffered studyregion shp
locale_4326_shp = os.path.join(locale_dir,'{}_{}_{}m_epsg4326'.format(locale.lower(),study_region,study_buffer))

command = (
      ' ogr2ogr -f "ESRI Shapefile" {out_feature}.shp  '
      ' -s_srs "EPSG:{from_epsg}" -t_srs "EPSG:{to_epsg}" ' 
      ' PG:"host={host} port=5432 dbname={db}'
      ' user={user} password = {pwd}" '
      ' "{table}" '.format(host = db_host,
                                   db = db,
                                   user = db_user,
                                   pwd = db_pwd,
                                   out_feature = locale_4326_shp,
                                   from_epsg = srid,
                                   to_epsg = 4326,
                                   table = buffered_study_region) 
      )
print(command)
sp.call(command, shell=True,cwd=os.path.dirname(os.path.join(folderPath,clean_intersections_gpkg)))

# output to completion log					
script_running_log(script, task, start, locale)
conn.close()
