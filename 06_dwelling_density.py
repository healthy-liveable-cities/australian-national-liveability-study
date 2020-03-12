# Script:  DwellingDensity.py
# Purpose: This script calculates dwelling density (dwellings per hectare)
#          summing dwellings in meshblocks intersecting sausage buffers
#
#          It creates an SQL table containing: 
#            parcel identifier, 
#            sausage buffer area in sqm 
#            sausage buffer area in ha 
#            dwelling count, and 
#            dwellings per hectare (dwelling density)
#
#          It requires that the sausagebuffer_1600 and area_linkage scripts have been run
#          as it uses the intersection of these spatial database features 
#          to aggregate dwelling counts within meshblocks intersecting sausage buffers
#
# Author:  Carl Higgs 20/03/2017

import time
import psycopg2
from progressor import progressor

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'calculate dwelling density (dwellings per hectare)'

# schema where point indicator output tables will be stored
schema = ind_point_schema

meshblock_table = "area_linkage"
buffer_table = "nh{}m".format(distance)
dd_table = 'dd_{}'.format(buffer_table)

createTable_dd = '''
  --DROP TABLE IF EXISTS {schema}.{table};
  CREATE TABLE IF NOT EXISTS {schema}.{table}
  ({id} {type} PRIMARY KEY,
   dwellings integer,
   area_ha double precision,
   mb_area_ha double precision,
   dd_nh1600m double precision
  ); 
  '''.format(schema = schema,
             table = dd_table,
             id  = points_id.lower(),
             type = points_id_type)
  
query_A = '''
INSERT INTO {schema}.{table} ({id},dwellings,area_ha,mb_area_ha,dd_nh1600m)
(SELECT s.{id},  
        sum(dwelling) AS dwellings,
        s.area_ha,
        sum(t.dwelling)/s.area_ha::double precision as dd_nh1600m
  FROM {schema}.{buffer_table} s
  LEFT JOIN {meshblock_table} t ON ST_intersects(s.geom, t.geom)
  WHERE s.{id} IN
'''.format(schema = schema,
           table = dd_table,
           id = points_id.lower(),
           buffer_table = buffer_table,
           meshblock_table = meshblock_table)
  
query_C = '''
  GROUP BY s.{id},s.area_ha) ON CONFLICT DO NOTHING;
  '''.format(id = points_id.lower())

#  Size of tuple chunk sent to postgresql 
sqlChunkify = 500
    
# Connect to postgreSQL server
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()
print("Connection to SQL success {}".format(time.strftime("%Y%m%d-%H%M%S")) )

# create dwelling density table
print("create table {}... ".format(dd_table)),
subTaskStart = time.time()
curs.execute(createTable_dd)
conn.commit()
print("{:4.2f} mins.".format((time.time() - start)/60))	
 
try:   
  print("fetch list of processed parcels, if any..."), 
  # (for string match to work, had to select first item of returned tuple)
  sql = '''
    SELECT {id}::text 
      FROM {schema}.{nh_geom} 
      WHERE {id} NOT IN (SELECT {id} FROM {schema}.dd_nh1600m);
      '''.format(id = points_id.lower(),
                 schema = schema,
                 nh_geom  = buffer_table)
  curs.execute(sql)
  point_id_list = [x[0] for x in  list(curs)]
  print("Done.")
  
  if len(point_id_list) > 0:   
    denom = len(point_id_list)
    count = 0
    chunkedPoints = list()
    
    print("Processing points...")
    for point in point_id_list:
      count += 1
      chunkedPoints.append(point) 
      if (count % sqlChunkify == 0) :
          curs.execute('{} ({}) {}'.format(query_A,','.join("'"+x+"'" for x in chunkedPoints),query_C))
          conn.commit()
          chunkedPoints = list()
          progressor(count,denom,start,"{}/{} points processed".format(count,denom))
    if(count % sqlChunkify != 0):
       curs.execute('{} ({}) {}'.format(query_A,','.join("'"+x+"'" for x in chunkedPoints),query_C))
       conn.commit()
    
    progressor(count,denom,start,"{}/{} points processed".format(count,denom))
  if len(point_id_list) == 0:
    print("All point ids from the sausage buffer table are already accounted for in the dwelling density table; I think we're done, so no need to re-run this script!")
  
except:
       print("HEY, IT'S AN ERROR:")
       print(sys.exc_info())

finally:       
  # output to completion log    
  script_running_log(script, task, start, locale)

  # clean up
  conn.close()