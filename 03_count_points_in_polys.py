# Script:  03_count_points_in_polys.py
# Purpose: To count highlife points within buildings

#  NOTE - this script is not generalised; it applies to highlife study, hardcoded

import time
import psycopg2 

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'count points in polygons'
print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))

conn = psycopg2.connect(database=db, 
                        user=db_user,
                        password=db_pwd)
curs = conn.cursor()
# note - this used to be hex_parels; may have to rename elsewhere to
# to generalised 'poly_points'
sql = '''
DROP TABLE IF EXISTS poly_points;
CREATE TABLE poly_points AS 
  SELECT t2.*, 
  (percent_rank() OVER win)::numeric(10,2) AS percentile 
  FROM (SELECT t1.*, 
               sum(count) OVER (ORDER BY {polygon_id}) AS cumfreq 
          FROM (SELECT {polygon_id}, 
                       COUNT(*) 
                  FROM {sample_point_feature} 
                  GROUP BY {polygon_id} ORDER BY {polygon_id}
                  ) t1
        ) t2 
  WINDOW win AS (ORDER BY cumfreq);
'''.format(sample_point_feature=sample_point_feature,
           polygon_id = polygon_id)
curs.execute(sql)
conn.commit()

script_running_log(script, task, start, locale)
conn.close()
