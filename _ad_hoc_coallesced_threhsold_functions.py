
#import packages
import os
import sys
import time
import numpy as np
import psycopg2 
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create coalesced threhsold functions (null automatically to zero score)'

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))

sql = '''
 CREATE OR REPLACE FUNCTION threshold_coalesce_hard(in int, in int, out int) 
    AS $$ SELECT COALESCE(($1 < $2)::int,0) $$
    LANGUAGE SQL;

-- a soft threshold indicator (e.g. of access given distance and threshold)
CREATE OR REPLACE FUNCTION threshold_coalesce_soft(distance int, threshold int) returns float AS 
$$
BEGIN
  -- We check to see if the value we are exponentiation is more or less than 100; if so,
  -- if so the result will be more or less either 1 or 0, respectively. 
  -- If the value we are exponentiating is much > abs(700) then we risk overflow/underflow error
  -- due to the value exceeding the numerical limits of postgresql
  -- If the value we are exponentiating is based on a positive distance, then we know it is invalid!
  -- For reference, a 10km distance with 400m threshold yields a check value of -120, 
  -- the exponent of which is 1.30418087839363e+052 and 1 - 1/(1+exp(-120)) is basically 1 - 1 = 0
  -- Using a check value of -100, the point at which zero is returned with a threshold of 400 
  -- is for distance of 3339km
  --
  -- Because there are instances where nulls may arise because we don't measure to full distance
  -- e.g. for some open space subset queries, we need to allow for these to be legitimate zero, not nulls
  -- Hence, this function returns 0 on Null input
  IF (distance IS NULL) 
      THEN RETURN 0;
  ELSIF (distance < 0) 
      THEN RETURN NULL;
  ELSIF (-5*(distance-threshold)/(threshold::float) < -100) 
    THEN RETURN 0;
  ELSE 
    RETURN 1 - 1/(1+exp(-5*(distance-threshold)/(threshold::float)));
  END IF;
END;
$$
LANGUAGE plpgsql; 
'''
engine.execute(sql)
engine.dispose()

# output to completion log
script_running_log(script, task, start)
engine
