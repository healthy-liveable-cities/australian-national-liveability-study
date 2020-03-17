"""

Compile destinations
~~~~~~~~~~~~~~~~~~~~

::

    Script:  06_compile_destinations.py
    Purpose: Compile a schema of destination features
    Authors: Carl Higgs 

"""

import arcpy
import time
import psycopg2
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])

# Destinations schema
schema = destinations_schema

# Compile restricted gdb of destination features
task = 'Compile study region destinations'
print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db), 
                       use_native_hstore=False)

# check that all destination names are unique; if not we'll have problems:
if df_destinations.destination.is_unique is not True:
  sys.exit("Destination names in 'destinations' tab of _project_configuration.xlsx are not unique, but they should be.  Please fix this, push the change to all users alerting them to the issue, and try again.");

df = df_destinations.set_index('destination').copy()

# list destinations which have OpenStreetMap specified as their data source
dest_osm_list = [x.encode('utf') for x in df_osm_dest.destination.unique().tolist()]
df_osm_dest['condition'] = df_osm_dest['condition'].replace('NULL','OR')
df_osm = df_osm_dest.copy()

# Create destination caralog as a summary of all destinations in study region
sql = '''
  DROP TABLE IF EXISTS destination_catalog;
  CREATE TABLE destination_catalog
  (
   destination varchar,
   name varchar,
   domain varchar NOT NULL,
   count integer,
   data varchar,
   process_od varchar,
   PRIMARY KEY (destination, name, data)
  );
   '''
engine.execute(sql)

# The destination schema is used for storing destination features in the study region
sql = '''
  CREATE SCHEMA IF NOT EXISTS {schema};
'''.format(schema=schema)
engine.execute(sql)


# get bounding box of buffered study region for clipping external data using ogr2ogr on import
sql = '''SELECT ST_Extent(geom) FROM {};'''.format(buffered_study_region)
urban_region = engine.execute(sql).fetchone()
urban_region = [float(x) for x in urban_region[0][4:-1].replace(',',' ').split(' ')]
bbox =  '{} {} {} {}'.format(*urban_region)

print("Importing pre-processed dest_gdb destinations...")
# pre-processed destinations
dest_processed_list =  sp.check_output('ogrinfo {}'.format(src_destinations)).split('\r\n')
dest_processed_list = [x[(x.find(' ')+1):x.find(' (Point)')] for x in dest_processed_list[ dest_processed_list.index(''.join([x for x in dest_processed_list if x.startswith('1:')])):-1]]

command = (
   ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
   ' PG:"host={db_host} port={db_port} dbname={db} active_schema={schema}'
   ' user={db_user} password = {db_pwd}" '
   ' {gdb} -clipsrc {bbox} '
   ' -lco geometry_name="geom" -lco OVERWRITE=YES -lco FID="dest_oid" '
   ).format(db_host=db_host,
            db_port=db_port,
            db=db,
            schema=schema,
            db_user=db_user,
            db_pwd=db_pwd,
            gdb = src_destinations,
            bbox=bbox
            )
# print(command)
sp.call(command, shell=True)

print("Creating feature dataset to hold destinations..."),
if not arcpy.Exists(os.path.join(gdb_path,destinations_schema)):
    arcpy.CreateFeatureDataset_management(gdb_path,
                                          destinations_schema, 
                                          spatial_reference = SpatialRef)
print(" Done.")

processing_gpkg = os.path.join(folderPath,'study_region',locale,'{}_processing.gpkg'.format(locale))

arcpy.env.workspace = gdb_path
arcpy.env.overwriteOutput = True 

print("\nProcessing destinations...")
print("\n{:25} {:40} {}".format("Destination","Name","Imported"))
for row in df.itertuples():
    destination = getattr(row,'Index')
    data = getattr(row,'data')
    name = getattr(row,'dest_name_full')
    print("\n{:25} {:40} ".format(destination,name)),
    domain = getattr(row,'domain')
    process_od = getattr(row,'process_od')
    if data == 'osm':          
        dest_condition = []
        for condition in ['AND','OR','NOT']:
        # for condition in df_osm_dest[df_osm_dest['destination']==destination]['condition'].unique():
            # print(condition)
            if condition == 'AND':
                clause = ' AND '.join(df_osm[(df_osm['destination']==destination)&(df_osm['condition']=='AND')].apply(lambda x: "{} IS NOT NULL".format(x.key) if x.value=='NULL' else "{} = '{}'".format(x.key,x.value),axis=1).values.tolist())
                dest_condition.append(clause)
            if condition == 'OR':
                clause = ' OR '.join(df_osm[(df_osm['destination']==destination)&(df_osm['condition']=='OR')].apply(lambda x: "{} IS NOT NULL".format(x.key) if x.value=='NULL' else "{} = '{}'".format(x.key,x.value),axis=1).values.tolist())
                dest_condition.append(clause)
            if condition != 'NOT':
                clause = ' AND '.join(df_osm[(df_osm['destination']==destination)&(df_osm['condition']=='NOT')].apply(lambda x: "{} IS NOT NULL".format(x.key) if x.value=='NULL' else "{} != '{}' OR access IS NULL".format(x.key,x.value),axis=1).values.tolist())
                dest_condition.append(clause)
        dest_condition = [x for x in dest_condition if x!='']
        # print(len(dest_condition))
        if len(dest_condition)==1:
            dest_condition = dest_condition[0]
        else:
            dest_condition = '({})'.format(') AND ('.join(dest_condition))
        # print(dest_condition)
        sql = '''
          DROP TABLE IF EXISTS {schema}.{destination};
          CREATE TABLE {schema}.{destination} AS
          SELECT (row_number() over())::int dest_oid,
                 osm_id,
                 "data",
                 geom
          FROM
          (SELECT * FROM
            (SELECT osm_id,
                    '{osm_prefix}_point' AS data,
                    geom
               FROM {osm_schema}.{osm_prefix}_point d WHERE {dest_condition}) point
             UNION
            (SELECT osm_id,
                    '{osm_prefix}_polygon' AS data,
                    ST_Centroid(d.geom)
               FROM {osm_schema}.{osm_prefix}_polygon d WHERE {dest_condition})
           ORDER BY data, osm_id) t
           ;
        '''.format(schema=schema,
                   destination=destination,
                   osm_schema=osm_schema,
                   osm_prefix=osm_prefix,
                   dest_condition=dest_condition)       
        # print(sql)
        engine.execute(sql)
    
    if not engine.has_table(destination,schema=schema):
        print("{count:=10d}".format(count=0))
    else:
        # catalogue destinations from this data source
        sql = '''
              SELECT count(*) FROM {schema}.{destination};
              '''.format(schema=schema,
                         destination=destination)
        count = engine.execute(sql).fetchone()[0]
        sql = '''
            INSERT INTO destination_catalog (destination,name,domain,count,data,process_od)
            SELECT '{destination}',
                   '{name}',
                   '{domain}',
                    {count},
                   '{data}',
                   '{process_od}'
            ON CONFLICT (destination, name, data) DO NOTHING;
        '''.format(destination = destination ,
                   name        = name        ,
                   domain      = domain      ,
                   count       = count       ,
                   data        = data        ,
                   process_od  = process_od ) 
        engine.execute(sql)
        print("{count:=10d}".format(count=count))
        sql = ''' SELECT EXISTS ( SELECT 1 FROM information_schema.columns 
                  WHERE table_name ='{destination}' AND table_schema = '{schema}' 
                  AND column_name='dest_oid' )
              '''.format(destination=destination,schema=schema)
        has_dest_oid = engine.execute(sql).fetchone()[0]
        if not has_dest_oid:
            # this could occur if table was a custom build imported in previous script
            sql = '''
              ALTER TABLE {schema}.{destination} ADD COLUMN IF NOT EXISTS dest_oid SERIAL;
              '''.format(schema=schema,destination=destination)
            engine.execute(sql)
        sql = '''
          CREATE INDEX IF NOT EXISTS {destination}_idx ON {schema}.{destination} (dest_oid);
          CREATE INDEX IF NOT EXISTS {destination}_gix ON {schema}.{destination} USING GIST (geom);
          '''.format(schema=schema,destination=destination)
        engine.execute(sql)
        if count > 0:
            # print("Exporting to arcpy via processing gpkg")
            command = (
                    ' ogr2ogr -overwrite -f GPKG  '
                    ' {gpkg} ' 
                    ' PG:"host={host} port=5432 dbname={db} user={user} password = {pwd} active_schema={schema}" '
                    ' {table} '.format(gpkg = processing_gpkg,
                                        schema = schema,
                                        host = db_host,
                                        db = db,
                                        user = db_user,
                                        pwd = db_pwd,
                                        table = destination
                                        )
            )                              
            sp.call(command, shell=True)
            arcpy.CopyFeatures_management('{}/{}'.format(processing_gpkg,destination), os.path.join(gdb_path,destinations_schema,destination))
print("Done.")
engine.execute('''CLUSTER destination_catalog USING destination_catalog_pkey;''')
  
# output to completion log    
script_running_log(script, task, start, locale)
engine.dispose()