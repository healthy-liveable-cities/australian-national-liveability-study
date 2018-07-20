# Script:  19_area_indicators.py
# Purpose: Create area level indicator tables
# Author:  Carl Higgs 
# Date:    20 July 2018


#### Sketch!! 

import os
import sys
import time
import psycopg2 

'''
SELECT state_name,
       ssc_code_2016,
       ssc_name_2016,
       ROUND(AVG(pos_2_wa_perth_2016_hard) * 100,2) AS pcent_pos_300m_any,
       ROUND(AVG(pos_10_WA_perth_2016_hard)* 100,2) AS pcent_pos_400m_any,
       ROUND(AVG(pos_3_WA_perth_2016_hard) * 100,2) AS pcent_pos_400m_local,
       ROUND(AVG(pos_4_WA_perth_2016_hard) * 100,2) AS pcent_pos_800m_neigh,
       ROUND(AVG(pos_5_WA_perth_2016_hard) * 100,2) AS pcent_pos_2km_district,
       ROUND(AVG(pos_11_WA_perth_2016_hard)* 100,2) AS pcent_pos_400m_1p5ha
 FROM parcel_indicators
 GROUP BY state_name,ssc_code_2016,ssc_name_2016
 ORDER BY ssc_name_2016 ASC;
 '''

curs.execute(createTable)
conn.commit() 
  
conn.close()
  
# output to completion log    
script_running_log(script, task, start, locale)
