# Purpose: Import sample points for study region and associate with assets
# Author:  Carl Higgs
# Date:    2020 06 03


import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import time
import psycopg2
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Clip address to study region, dissolve by location counting collapse degree'

engine = create_engine(f"postgresql://{db_user}:{db_pwd}@{db_host}/{db}")

if not engine.dialect.has_table(engine, 'parcel_dwellings'):  
    engine.execute(f'''DROP TABLE IF EXISTS temp_parcels; DROP TABLE IF EXISTS temp_parcels_2;''')
    sql = f'''SELECT ST_Extent(geom) FROM {study_region};'''
    # get bounding box of buffered study region for clipping external data using ogr2ogr on import
    urban_region = engine.execute(sql).fetchone()
    urban_region = [float(x) for x in urban_region[0][4:-1].replace(',',' ').split(' ')]
    bbox =  '{} {} {} {}'.format(*urban_region)
    for feature in points:  
        print("Processing point source '{}'...".format(feature))
        data = f'{folderPath}/{feature}'
        gdb = os.path.dirname(data)
        sample_points = os.path.basename(data)
        # note assumes geometry column is 
        command = (
                  f'ogr2ogr -append -progress -f "PostgreSQL" ' 
                  f'PG:"host={db_host} port={db_port} dbname={db} '
                  f'user={db_user} password = {db_pwd} " '
                  f'{gdb} "{sample_points}"  -clipsrc {bbox} '
                  f'-a_srs "EPSG:{points_srid} " '
                  f'-t_srs "EPSG:{srid} " '
                  f'-lco geometry_name="geom" -nln "temp_parcels"'
                  )
        print(command)
        # sp.call(command, shell=True)
    print(f"Creating derived {parcel_dwellings} layer..."),
    sql = f'''
        DELETE FROM temp_parcels a 
              USING study_region b 
              WHERE NOT ST_Intersects(a.geom,b.geom);
        CREATE INDEX temp_parcels_idx ON temp_parcels ({points_id});
        CREATE TABLE temp_parcels_2 AS
        SELECT  a.{points_id}           ,
               ST_X(a.geom) AS point_x,
               ST_Y(a.geom) AS point_y,
               a.geom
        FROM (SELECT DISTINCT ON (geom)
               {points_id}, geom
                FROM   temp_parcels
                ORDER BY geom, {points_id}) a;
        CREATE INDEX temp_parcels_2_idx ON temp_parcels_2 ({points_id});
        CREATE INDEX temp_parcels_2_gix ON temp_parcels_2 USING GIST (geom);
        CREATE TABLE parcel_dwellings AS    
        SELECT a.{points_id}           ,
               b.mb_code_2016       ,
               c.objectid AS hex_id,
               a.point_x,
               a.point_y,
               a.geom
        FROM temp_parcels_2 a
        LEFT JOIN area_linkage b ON ST_Intersects(a.geom,b.geom)
        LEFT JOIN boundaries.study_region_hex_3000m_diag c ON ST_Intersects(a.geom,c.geom);
        CREATE UNIQUE INDEX parcel_dwellings_idx ON parcel_dwellings ({points_id});
        CREATE INDEX parcel_dwellings_gix ON parcel_dwellings USING GIST (geom);
        DROP TABLE temp_parcels;
        DROP TABLE temp_parcels_2;
        '''
    engine.execute(sql)
    print("Done.")
    print("Creating hex_parcels table counting parcels within hexes"),
    sql ='''
    DROP TABLE IF EXISTS hex_parcels;
    CREATE TABLE hex_parcels AS 
    SELECT hex,
           count,
           cumfreq,
           ROUND((100*cumfreq / (SELECT COUNT(*) FROM parcel_dwellings)),2) AS percentile
    FROM (SELECT hex,
                 count,
                 sum(count) OVER (ORDER BY hex) AS cumfreq
          FROM (SELECT hex_id AS hex,
                 COUNT(*)
                 FROM parcel_dwellings
                 GROUP BY hex_id) absolute
          ) relative;
    '''
    # grant access to the tables just created
    engine.execute(grant_query)
    print("Done.")
    engine.dispose()
    
# copy parcel dwelling sample points to arcgis gdb
import arcpy
arcpy.env.workspace = db_sde_path
arcpy.env.overwriteOutput = True

arcpy.CopyFeatures_management(f'public.{parcel_dwellings}',f'{gdb_path}/{parcel_dwellings}')

# # output to completion log					
script_running_log(script, task, start, locale)



