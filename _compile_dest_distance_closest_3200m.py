# Script:  16_neighbourhood_indicators.py
# Purpose: Compile destinations results and neighbourhood indicator tables
# Author:  Carl Higgs 
# Date:    20190412

import os
import sys
import time
import psycopg2 

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

print('''
This script will combine the 'dest_distance_m' (distance to closest) and 
dest_distances_3200m (distance to all in 3200m) tables.  

The resulting table will contain distances to all destinations for each 
class within 3200 metres, and/or the distance to closest (which may be
further than 3200 metres).

As such questions concerning 'distance to closest' and 'all distances' 
within some threshold up to 3200m may be determined using this new
table.

In future the plan is that these two source tables will be calculated
as a combined table in the first instance.  However, our approach to 
date has differentiated 'dest_class' (a conceptual category) from 
'dest_name' (the name of the particular datasource, for which there may 
be multiple associated with a category, in theory) and as such this has 
not been possible without a more extensive script re-write.

Hence, this utility script.  

Let's go!
''')

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

print('Creating or replacing array_append_if_gr function ... '),
curs.execute('''
CREATE OR REPLACE FUNCTION array_append_if_gr(distances int[],distance int,threshold int default 3200) returns int[] as $$
BEGIN
-- function to append an integer to an array of integers if it is larger than some given threshold 
-- (ie. add in distance to closest to 3200m distances array if the distance to closest value is > 3200m
-- Example applied usage:
-- SELECT gnaf_pid, 
        -- array_append_if_gr(dests.alcohol_offlicence,cl.alcohol_offlicence) AS array,
        -- cl.alcohol_offlicence AS distance
-- FROM dest_distances_3200m dests 
-- LEFT JOIN dest_distance_m cl
-- USING (gnaf_pid) 
-- WHERE cl.alcohol_offlicence > 3200;
IF ((distance <= threshold) OR (distance IS NULL)) 
    THEN RETURN distances;
ELSE 
    RETURN array_append(distances,distance);
END IF;
END;
$$
LANGUAGE plpgsql;  
''')
conn.commit()
print('Done.')
# Restrict to indicators associated with study region (except distance to closest dest indicators)
ind_matrix = df_inds[df_inds['locale'].str.contains('|'.join([locale,'\*']))]

print("Combining tables..."),
# Get a list of all potential destinations for distance to closest 
# (some may not be present in region, will be null, so we can refer to them in later queries)
# destination names
categories = [x for x in df_destinations.destination.tolist()]
array_categories = [x for x in df_destinations.destination_class.tolist()]
destinations = ',\n'.join(['''array_append_if_gr(dests."{1}",cl."{0}") AS "{1}"'''.format(*dest) for dest in zip(categories,array_categories)])
table = 'dest_distances_cl_3200m'
curs.execute('''
CREATE TABLE {table} AS
SELECT gnaf_pid, 
       {destinations}
FROM dest_distances_3200m dests 
LEFT JOIN dest_distance_m cl
USING (gnaf_pid);
CREATE UNIQUE INDEX IF NOT EXISTS {table}_idx ON  {table} ({id}); 
'''.format(table = table,
           id = points_id.lower(),
           destinations = destinations))
conn.commit()
print(" Done.")

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
