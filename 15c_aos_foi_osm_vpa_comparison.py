# Script:  15_od_aos_testing_melb_vpa.py
# Purpose: Calcault distance to nearest AOS within 3.2km, 
#          or if none within 3.2km then distance to closest
#
#          This is a test implementation of the script which facilitates
#          comparisons with OSM and VicMap networks for accessing 
#          POS constructed using VPA and FOI data, or open spaces (OS) using OSM
#          In the case of OSM, a post-processing script narrows down to evaluate 
#          access to the subset of AOS that contain OS meeting definition of POS
# Authors: Carl Higgs

import arcpy, arcinfo
import os
import time
import multiprocessing
import sys
import psycopg2 
from sqlalchemy import create_engine
import numpy as np
from progressor import progressor

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Compile a table comparing indicator results using various network and pos source combinations'

pos_400m_comparison = '''
gnaf_pid
vicmap_foi_any
vicmap_foi_gr1ha
vicmap_vpa_any
vicmap_vpa_gr1ha
vicmap_osm_any
vicmap_osm_gr1ha
osm_foi_any
osm_foi_gr1ha
osm_vpa_any
osm_vpa_gr1ha
osm_osm_any
osm_osm_gr1ha
'''

  
# output to completion log    
script_running_log(script, task, start, locale)
conn.close()