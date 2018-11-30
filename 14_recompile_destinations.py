# Script:  recompile_destinations_gdb.py
# Purpose: This script recompiles the destinations geodatabase:
#             - converts multi-point to point where req'd
#             - clips to study region
#             - restricts to relevant destinations
#             - removes redundant columns
#             - compile as a single feature.
#             - A point ID is comma-delimited in form "Destionation,OID"
#               - this is to facilitate output to csv file following OD matrix calculation
#
# Author:  Carl Higgs
# Date:    05/07/2018

# import arcpy
import time
# import numpy
# import json
import psycopg2
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])

# OUTPUT PROCESS
# Compile restricted gdb of destination features
task = 'Recompile destinations from {} to study region gdb as combined feature {}'.format(dest_gdb,os.path.join(gdb,outCombinedFeature))
print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))

# check that all destination names are unique; if not we'll have problems:
if df_destinations.destination.is_unique is not True:
  sys.exit("Destination names in 'destinations' tab of ind_study_region_matrix.xlsx are not unique, but they should be.  Please fix this, push the change to all users alerting them to the issue, and try again.");

# Compile a list of datasets to be checked over for valid features within the destination GDB
datasets = arcpy.ListDatasets(feature_type='feature')

dest_not_osm = df_destinations[df_destinations['data_source']!='OpenStreetMap']
dest_osm = df_destinations[df_destinations['data_source']=='OpenStreetMap']
dest_osm_list = dest_osm.destination.tolist()

# create new feature for combined destinations using a template
# Be aware that if the feature does exist, it will be overwritten
# If you are just wanting to add more destinations after having previously processed 
# you should comment the following two commands out, or place the following them
# in a condition such as the following:
# if arcpy.Exists(os.path.join(gdb_path,outCombinedFeature)) is False:  
arcpy.CreateFeatureclass_management(out_path = gdb_path, 
                                    out_name = outCombinedFeature,
                                    template = combined_dest_template)      
# Define projection to study region spatial reference
arcpy.DefineProjection_management(os.path.join(gdb_path,outCombinedFeature), spatial_reference)

# Create destination type table in sql database
# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()

### New idea for processing in PostGIS
# list features for which we appear to have data
# pre-processed destinations
dest_processed_list =  sp.check_output('ogrinfo {}'.format(src_destinations)).split('\r\n')
dest_processed_list = [x[(x.find(' ')+1):x.find(' (Point)')] for x in dest_processed_list[ dest_processed_list.index(''.join([x for x in dest_processed_list if x.startswith('1:')])):-1]]
# osm destinations
dest_osm_list = dest_osm.destination.tolist()

print("Temporarily copy all pre-processed destinations to postgis..."),
command = (
        ' ogr2ogr -overwrite -progress -f "PostgreSQL" ' 
        ' PG:"host={host} port=5432 dbname={db}'
        ' user={user} password = {pwd}" '
        ' {gdb} '
        ' -lco geometry_name="geom"'.format(host = db_host,
                                     db = db,
                                     user = db_user,
                                     pwd = db_pwd,
                                     gdb = src_destinations) 
        )
print(command)
sp.call(command, shell=True)
print("Done (although, if it didn't work you can use the printed command above to do it manually)")

# Create empty combined destination table
create_dest_type_table = '''
  DROP TABLE IF EXISTS dest_type;
  CREATE TABLE dest_type
  (dest_class varchar NOT NULL,
   dest_name varchar PRIMARY KEY,
   dest_name_full varchar,
   domain varchar NOT NULL,
   count integer,
   cutoff_closest integer,
   cutoff_count integer);
   '''
curs.execute(create_dest_type_table)
conn.commit()

create_study_destinations_table = '''
  DROP TABLE IF EXISTS study_destinations;
  CREATE TABLE study_destinations
  (dest_oid varchar NOT NULL PRIMARY KEY,
   dest_name varchar NOT NULL,
   geom geometry(POINT));
  CREATE INDEX study_destinations_dest_name_idx ON study_destinations (dest_name);
  CREATE INDEX study_destinations_geom_geom_idx ON study_destinations USING GIST (geom);
'''
curs.execute(create_study_destinations_table)
conn.commit()
print("Processing"),
for dest in destination_list:
  print("."),
  dest_fields = {}
  for field in ['destination_class','dest_name_full','domain','cutoff_closest','cutoff_count']:
    dest_fields[field] = df_destinations.loc[df_destinations['destination'] == dest][field].to_string(index = False).encode('utf')
  if dest in dest_processed_list:
    limit_extent = '''
      DELETE 
        FROM {} d 
       USING {} b
       WHERE NOT ST_Intersects(d.geom,b.geom);
    '''.format(dest,buffered_study_region)
    curs.execute(limit_extent)
    conn.commit()
    
    # count destinations from this source within the study region
    curs.execute('''SELECT count(*) FROM {};'''.format(dest))
    dest_count = int(list(curs)[0][0])     
    
    # make sure all geom are point
    enforce_point = '''
      UPDATE {} 
         SET geom = ST_Centroid(geom)
       WHERE ST_GeometryType(geom) != 'ST_Point';
    '''.format(dest)
    curs.execute(enforce_point)
    conn.commit()
    
    # get primary key, this would ideally be 'objectid', but we can't assume
    get_primary_key_field = '''
      SELECT a.attname
      FROM   pg_index i
      JOIN   pg_attribute a ON a.attrelid = i.indrelid
                           AND a.attnum = ANY(i.indkey)
      WHERE  i.indrelid = '{}'::regclass
      AND    i.indisprimary;
    '''.format(dest)
    curs.execute(get_primary_key_field)
    dest_pkey = list(curs)[0][0]
    
    summarise_dest_type = '''
    INSERT INTO dest_type (dest_class,dest_name,dest_name_full,domain,count,cutoff_closest,cutoff_count)
    SELECT '{dest_class}',
           '{dest_name}',
           '{dest_name_full}',
           '{domain}',
           (SELECT count(*) FROM {})::text,
           '{cutoff_closest}',
           '{cutoff_count}'
    '''.format(dest_class    = dest_fields['destination_class'],
               dest_name     = dest_fields[dest],
               dest_name_full= dest_fields['dest_name_full'],
               domain        = dest_fields['domain'],
               cutoff_closest= dest_fields['cutoff_closest'],
               cutoff_count  = dest_fields['cutoff_count'])
    curs.execute(summarise_dest_type)
    conn.commit()
    
    combine_destinations = '''
      INSERT INTO study_destinations (dest_oid,dest_name,geom)
      SELECT '{class},'||{key}, dest_name, geom FROM {dest};
    '''.format(dest_class = dest_fields['destination_class'],
               dest_pkey = dest_pkey,
               dest = dest)
    # import dest to db as tempdest table from 
    # either pre-processed dest source 
    # (gdb for now, but later replace with gpkg)
  
  elif dest in dest_osm_list:
  
  else:
    print("{} does not have coverage in this studyregion, or the datasource does not otherwise appear to be available.")


print('''Ingesting pre-processed destinations located in destinations geodatabase  ...''')                    
for ds in datasets:
  for fc in arcpy.ListFeatureClasses(feature_dataset=ds):
    if fc in destination_list:
      # destNum = destination_list.index(fc)
      dest_class = dest_not_osm.loc[dest_not_osm['destination'] == fc]['destination_class'].to_string(index = False).encode('utf')
      # Make sure all destinations conform to shape type 'Point' (ie. not multipoint)
      if arcpy.Describe(fc).shapeType != u'Point':
        arcpy.FeatureToPoint_management(fc, scratchOutput, "INSIDE")
        arcpy.MakeFeatureLayer_management(scratchOutput,'destination')  
      else:
        # Select and copy destinations intersecting Melbourne hexes
        arcpy.MakeFeatureLayer_management(fc,'destination')                                            
      # clip to hex grid buffer
      selection = arcpy.SelectLayerByLocation_management('destination', 'intersect',os.path.join(gdb_path,hex_grid_buffer))
      count = int(arcpy.GetCount_management(selection).getOutput(0))
      dest_count[fc] = count
      # Insert new rows in combined destination feature
      with arcpy.da.SearchCursor(selection,['SHAPE@','OID@']) as sCur:
        with arcpy.da.InsertCursor( os.path.join(gdb_path,outCombinedFeature),['SHAPE@','OBJECTID','dest_oid','dest_name']) as iCur:
          for row in sCur:
            dest_oid  = '{:02},{}'.format(dest_class,row[1])
            dest_name = fc.encode('utf8')
            iCur.insertRow(row+(dest_oid, dest_name))

      # arcpy.Append_management('featureTrimmed', os.path.join(gdb_path,outCombinedFeature))
      print("Appended {} ({} points)".format(fc,count))


print('''Ingesting OSM destinations...''')
for dest in dest_osm.destination.tolist():
  dest_condition = ' OR '.join(df_osm_dest[df_osm_dest['dest_name']==dest].apply(lambda x: "{} IS NOT NULL".format(x.key) if x.value=='NULL' else "{} = '{}'".format(x.key,x.value),axis=1).tolist())
  print('\n{} is defined as "{}"'.format(dest,dest_condition))
      
      
      
      
      

# drop table if it already exists
curs.execute("DROP TABLE IF EXISTS dest_type;")
conn.commit()
curs.execute(createTable)
conn.commit()

# insert values into table
# note that dest_count is feature count from above, not the dest_counts var from config
for i in range(0,len(destination_list)):
  curs.execute("INSERT INTO dest_type VALUES ({},'{}','{}',{},{},{})".format(dest_codes[i],
                                                                             destination_list[i],
                                                                             dest_domains[i],
                                                                             dest_count[i],  
                                                                             dest_cutoffs[i],
                                                                             dest_counts[i]) +' ON CONFLICT DO NOTHING')
  conn.commit()

print("Created 'dest_type' destination summary table for database {}.".format(db))
conn.close()
  
# output to completion log    
script_running_log(script, task, start, locale)