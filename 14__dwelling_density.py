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
#          It requires that the sausagebuffer_1600 and abs_linkage scripts have been run
#          as it uses the intersection of these spatial database features 
#          to aggregate dwelling counts within meshblocks intersecting sausage buffers
#
# Author:  Carl Higgs 20/03/2017

import time
import psycopg2
from progressor import progressor

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'calculate dwelling density (dwellings per hectare)'

meshblock_table = "abs_linkage"
buffer_table = "sausagebuffer_{}".format(distance)
nh_sausagebuffer_summary = "nh{}m".format(distance)
dd_table = 'dd_{}'.format(nh_sausagebuffer_summary)

createTable_dd = '''
  DROP TABLE IF EXISTS {0};
  CREATE TABLE IF NOT EXISTS {0}
  ({1} varchar PRIMARY KEY,
   dwellings integer NOT NULL,
   area_ha double precision NOT NULL,
   mb_area_ha double precision NOT NULL,
   dd_nh1600m double precision NOT NULL ,
   mb_dd_nh1600m double precision NOT NULL
  ); 
  '''.format(dd_table,points_id.lower())
  
query_A = '''
INSERT INTO {0} ({1},dwellings,area_ha,mb_area_ha,dd_nh1600m,mb_dd_nh1600m)
(SELECT s.{1},  
        sum(dwelling) AS dwellings,
        nh.area_ha,
        sum(t.area_ha) AS mb_area_ha,
        sum(t.dwelling)/nh.area_ha::double precision as dd_nh1600m,
        sum(t.dwelling)/sum(t.area_ha)::double precision AS mb_dd_nh1600m
  FROM {2} s
  LEFT JOIN {4} nh ON s.gnaf_pid = nh.gnaf_pid
  LEFT JOIN {3} t ON ST_intersects(s.geom, t.geom)
  WHERE s.{1} IN
'''.format(dd_table,points_id.lower(),buffer_table,meshblock_table,nh_sausagebuffer_summary)
  
query_C = '''
  GROUP BY s.{0},nh.area_ha) ON CONFLICT DO NOTHING;
  '''.format(points_id.lower())

#  Size of tuple chunk sent to postgresql 
sqlChunkify = 500
    
# Connect to postgreSQL server
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()
print("Connection to SQL success {}".format(time.strftime("%Y%m%d-%H%M%S")) )
# drop table if it already exists


# Create spatial indices if not already existing
print("Creating sausage buffer spatial index if not exists... ")
curs.execute("CREATE INDEX IF NOT EXISTS {0}_gix ON {0} USING GIST (geom);".format(buffer_table))
conn.commit()
print("Creating abs linkage (meshblock_table) spatial index if not exists... ")
curs.execute("CREATE INDEX IF NOT EXISTS {0}_gix ON {0} USING GIST (geom);".format(meshblock_table))
conn.commit()
print("Creating neighbourhood sausage buffer summary table ({}) primary key... ".format(nh_sausagebuffer_summary))
curs.execute("ALTER TABLE {0} ADD PRIMARY KEY ({1});".format(nh_sausagebuffer_summary,points_id.lower()))
conn.commit()
print("Done.")

# create dwelling density table
print("create table {}... ".format(dd_table)),
subTaskStart = time.time()
curs.execute(createTable_dd)
conn.commit()
print("{:4.2f} mins.".format((time.time() - start)/60))	
 
try:   
  print("fetch list of processed parcels, if any..."), 
  # (for string match to work, had to select first item of returned tuple)
  curs.execute("SELECT {} FROM {}".format(points_id.lower(),buffer_table))
  raw_point_id_list = list(curs)
  raw_point_id_list = [x[0] for x in raw_point_id_list]
  
  curs.execute("SELECT {} FROM {}".format(points_id.lower(),dd_table))
  completed_points = list(curs)
  completed_points = [x[0] for x in completed_points]
  
  point_id_list = [x for x in raw_point_id_list if x not in completed_points]  
  print("Done.")
  
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
  
except:
       print("HEY, IT'S AN ERROR:")
       print(sys.exc_info())

finally:       
  # output to completion log    
  script_running_log(script, task, start, locale)

  # clean up
  conn.close()