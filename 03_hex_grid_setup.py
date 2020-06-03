"""

Create sample points and hex grid
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    Script:  05_create_sample_points.py
    Purpose: Create hex grid and sample points
    Authors: Carl Higgs

"""

import time
from sqlalchemy import create_engine
import psycopg2

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

def main():
    # simple timer for log file
    start = time.time()
    script = os.path.basename(sys.argv[0])
    task = 'Create hex grid and sample points'

    conn = psycopg2.connect(database=db, user=db_user, password=db_pwd, host=db_host,port=db_port)
    curs = conn.cursor()
    
    engine = create_engine(f"postgresql://{db_user}:{db_pwd}@{db_host}/{db}",use_native_hstore=False)
    if not engine.dialect.has_table(engine, hex_grid,boundary_schema):  
        # Create hex grid using algorithm from Hugh Saalmans (@minus34) under Apache 2 licence
        # https://github.com/minus34/postgis-scripts/blob/master/hex-grid/create-hex-grid-function.sql
        curs.execute(hex_function)
        conn.commit() 
       
        # create hexes with some additional offsetting to ensure complete study region coverage
        sql = f'''
        CREATE TABLE IF NOT EXISTS {boundary_schema}.{hex_grid} AS
        SELECT row_number() OVER () AS objectid, geom
          FROM (
          SELECT hex_grid({hex_area_km2}::float, 
                          ST_XMin(geom)-{hex_buffer}, 
                          ST_YMin(geom)-{hex_buffer}, 
                          ST_XMax(geom)+{hex_buffer}, 
                          ST_YMax(geom)+{hex_buffer}, 
                          {srid}, 
                          {srid}, 
                          {srid}) geom, geom AS old_geom
        FROM {buffered_study_region}) t
        WHERE ST_Intersects(geom,old_geom);
        CREATE UNIQUE INDEX IF NOT EXISTS {hex_grid}_idx ON {boundary_schema}.{hex_grid} (objectid);
        CREATE INDEX IF NOT EXISTS {hex_grid}_geom_idx ON {boundary_schema}.{hex_grid} USING GIST (geom);
        '''     
        engine.execute(sql)
    else:
        print(f"  - The table {hex_grid} has already been processed.") 
    
        
    if not engine.dialect.has_table(engine, hex_grid_buffer,boundary_schema):  
        sql = f'''
        CREATE TABLE IF NOT EXISTS {boundary_schema}.{hex_grid_buffer} AS
        SELECT objectid AS orig_fid, 
               ST_Buffer(geom,{hex_buffer}) AS geom
          FROM {boundary_schema}.{hex_grid};
        CREATE UNIQUE INDEX IF NOT EXISTS {hex_grid_buffer}_idx ON {boundary_schema}.{hex_grid_buffer} (orig_fid);
        CREATE INDEX IF NOT EXISTS {hex_grid_buffer}_geom_idx ON {boundary_schema}.{hex_grid_buffer} USING GIST (geom);
        '''         
        engine.execute(sql)
    else:
        print(f"  - The table {hex_grid_buffer} has already been processed.") 

 
    # grant access to the tables just created
    engine.execute(grant_query)
    
    conn.close()
    engine.dispose()
    
    import arcpy
    # copy hex grids arcgis
    arcpy.env.workspace = db_sde_path
    arcpy.env.overwriteOutput = True

    arcpy.CopyFeatures_management(f'{boundary_schema}.{hex_grid}',f'{gdb_path}/{hex_grid}')
    arcpy.CopyFeatures_management(f'{boundary_schema}.{hex_grid_buffer}',f'{gdb_path}/{hex_grid_buffer}')
    
    # # output to completion log					
    script_running_log(script, task, start, locale)
    
if __name__ == '__main__':
    main()
    