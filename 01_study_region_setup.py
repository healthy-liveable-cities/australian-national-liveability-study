# Script:  area_linkage_tables.py
# Purpose: Create ABS and non-ABS linkage tables using 2016 data sourced from ABS
#
#          Parcel address points are already associated with Meshblock in the sample_point_feature table
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
from _project_setup import *


# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = '\nCreate area linkage tables using 2016 data sourced from ABS'

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db), 
                       use_native_hstore=False)

print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))
# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()

print("\nImport region data... "),
print("Import pre-processed data for the Highlife study... "),
tables = [['clean_intersections_12m'],
          ['edges'],
          ['nodes'],
          ['osm_20190902_line'],
          ['osm_20190902_point'],
          ['osm_20190902_polygon'],
          ['osm_20190902_roads'],
          ['{}_au_2019'.format(locale),                            'study_region'],
          ['{}_au_2019_10000m'.format(locale),                     'study_region_10000m'],
          ['{}_au_2019_hex_3200m_diag'.format(locale),             'study_region_hex_3200m_diag'],
          ['{}_au_2019_hex_3200m_diag_3200m_buffer'.format(locale),'study_region_hex_3200m_diag_3200m_buffer'],
          ['{}_AccessPts_Edited'.format(locale).lower()],
          ['{}_Footprints_v3'.format(locale).lower(),'footprints']
          ]

command = (
              ' ogr2ogr -overwrite -progress -f "PostgreSQL" '
              ' -t_srs "EPSG:{to_epsg}" ' 
              ' PG:"host={host} port=5432 dbname={db}'
              ' user={user} password = {pwd}" '
              ' "{gpkg}" '
              ' -lco geometry_name="geom"'.format(host = db_host,
                                           db = db,
                                           user = db_user,
                                           pwd = db_pwd,
                                           to_epsg = srid,
                                           gpkg = preprocessed_data) 
              )
print(command)
sp.call(command, shell=True,cwd=os.path.dirname(preprocessed_data))

for t in tables:
    if len(t) == 2:
        sql = '''ALTER TABLE IF EXISTS {} RENAME TO {}'''.format(t[0],t[1])
        engine.execute(sql)
        sql = '''ALTER INDEX IF EXISTS {}_fid_seq RENAME TO {}_fid_seq'''.format(t[0],t[1])
        engine.execute(sql)
        
# get bounding box of buffered study region for clipping external data using ogr2ogr on import

boundaries_schema='boundaries'
for geo in geo_imports.index.values:
  data = geo_imports.loc[geo,'data']
  if os.path.splitext(data)[1]=='.gpkg':
      epsg = int(geo_imports.loc[geo,'epsg'])
      sql = '''SELECT ST_Extent(ST_Transform(geom,{epsg})) 
                 FROM {buffered_study_region};
            '''.format(epsg = epsg,
                       buffered_study_region=buffered_study_region)
      # transform data if not in project spatial reference
      if epsg!=srid:
          transform =   ' -s_srs "EPSG:{epsg}" -t_srs "EPSG:{srid}" '.format(epsg=epsg,srid=srid)
      else:
          transform = ''
      urban_region = engine.execute(sql).fetchone()
      urban_region = [float(x) for x in urban_region[0][4:-1].replace(',',' ').split(' ')]
      bbox =  '{} {} {} {}'.format(*urban_region)
      command = (
               ' ogr2ogr -overwrite -progress -f "PostgreSQL" '
              ' PG:"host={db_host} port=5432 dbname={db} active_schema={boundaries_schema}'
              ' user={db_user} password = {db_pwd}" '
              ' "{data}"  -clipsrc {bbox}'
              ' -lco geometry_name="geom"'
              ' {transform}'
              ).format(db=db,
                       db_host=db_host,
                       db_user=db_user,
                       db_pwd=db_pwd,
                       data=data,
                       bbox=bbox,
                       transform=transform)
      print(command)
      sp.call(command, shell=True,cwd=os.path.dirname(os.path.join(folderPath)))
print("Done.")

print("Initiate area linkage table based on smallest region in region list (first entry: {})... )".format(geographies[0])),
print('''(note that a warning "Did not recognize type 'geometry' of column 'geom'" may appear; this is fine.)''')
sql = 'SELECT * FROM {} WHERE geom IS NOT NULL'.format(df_regions.loc[geographies[0],'table'])
area_linkage = pandas.read_sql(sql,con=engine,index_col=df_regions.loc[geographies[0],'id']).reset_index()

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

print("\nRecreate area linkage table geometries and GIST index... "),
# work around since geopandas isn't necessarily installed on our computers
sql = '''
---- commented out dwelling exclusion; this is arguably excessive for the linkage table
-- DELETE FROM area_linkage WHERE dwelling = 0;
ALTER TABLE area_linkage ADD COLUMN IF NOT EXISTS urban text;
ALTER TABLE area_linkage ADD COLUMN IF NOT EXISTS study_region boolean;
ALTER TABLE area_linkage ADD COLUMN IF NOT EXISTS area_ha double precision;
ALTER TABLE area_linkage ADD COLUMN IF NOT EXISTS geom geometry;
'''
curs.execute(sql)
conn.commit()
sql = '''
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
 CREATE INDEX gix_area_linkage ON area_linkage USING GIST (geom);
'''.format(table = df_regions.loc[geographies[0],'table'],
           id    = df_regions.loc[geographies[0],'id'])
curs.execute(sql)
conn.commit()

sql = '''
UPDATE area_linkage a 
   SET study_region = ST_CoveredBy(a.geom,s.geom)
  FROM study_region s;
  '''
curs.execute(sql) 
conn.commit()

sql ='''
DROP TABLE IF EXISTS mb_dwellings;
CREATE TABLE mb_dwellings AS 
      SELECT *
        FROM area_linkage
       WHERE dwelling > 0;
CREATE UNIQUE INDEX ix_mb_dwellings ON mb_dwellings (mb_code_2016);
CREATE INDEX gix_mb_dwellings ON mb_dwellings USING GIST (geom);
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
else:
  arcpy.CreateFileGDB_management(locale_dir,gdb)
  print("File geodatabase created: {}".format(gdb_path))

# copy study region, buffered study region and mb_dwellings to arcgis
arcpy.env.workspace = db_sde_path
arcpy.env.overwriteOutput = True

features = ['{}'.format(study_region),
            '{}'.format(buffered_study_region),
            sample_point_feature,
            'mb_dwellings']
# for feature in features:
    # print(feature)
    # try:
        # if arcpy.Exists('public.{}'.format(feature)):
            # arcpy.CopyFeatures_management('public.{}'.format(feature), os.path.join(gdb_path,feature))
        # else:
            # print("It seems that the feature doesn't exist...")
    # except:
       # print("... that didn't work ...")

## NOTE:
# The scripts copying of resources from Postgis using the arcpy spatial database engine (SDE) connection
# to a file geodatabase (GDB) for network analysis did not work, and nor did manually attempting to copy 
# from the SDE to the GDB.  
# In lieu of this, features were first exported to a 'processing' geopackage, 
# and then manually copied to each regions' GDB using ArcCatalog.  

print("Exporting to GPKG as intermediary step in case of automated copy failure; can then manually copy to the study region gdb")
features = ['area_linkage',
            '{}'.format(study_region),
            '{}'.format(buffered_study_region),
            sample_point_feature,
            'footprints',
            'edges',
            'nodes',
            'mb_dwellings']
processing_gpkg = os.path.join(folderPath,'study_region',locale,'{}_processing.gpkg'.format(locale))
command = (
        ' ogr2ogr -overwrite -f GPKG  '
        ' {gpkg} ' 
        ' PG:"host={host} port=5432 dbname={db} user={user} password = {pwd}" '
        ' {tables} '.format(gpkg = processing_gpkg,
                              host = db_host,
                              db = db,
                              user = db_user,
                              pwd = db_pwd,
                              tables = ' '.join(features)
                              )
)                              
sp.call(command, shell=True)
print("Done.")

for feature in features:
   print(feature)
   try:
       if arcpy.Exists('public.{}'.format(feature)):
           arcpy.CopyFeatures_management('{}/{}'.format(processing_gpkg,feature), os.path.join(gdb_path,feature))
       else:
           print("It seems that the feature doesn't exist...")
   except:
      print("... that didn't work ...")

# output to completion log					
script_running_log(script, task, start, locale)
engine.dispose()
