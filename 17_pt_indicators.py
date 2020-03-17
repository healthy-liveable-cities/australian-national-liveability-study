# Script:  _pt_indicators.py
# Purpose: Calculate post hoc PT indicators
# Authors: Carl Higgs
# Date: 2020-02-08

import os
import time
import sys
import psycopg2 
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger

from script_running_log import script_running_log

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Calculate post hoc public transport measure distances'

# Import custom variables for National Liveability indicator process
from _project_setup import *
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db), 
                       use_native_hstore=False)

sql = '''SELECT destination FROM destination_catalog WHERE process_od = 'ind_transport' '''
pt_points = pandas.read_sql(sql,engine)

## specify "destinations"
if pt_points.size ==0:
    sys.exit('Public transport for this analysis has not been defined; define an appropriate destination (GTFS stop locations) in the setup sheet as having a process_od value of "ind_transport" ')

pt_of_interest = {"pt_any"             :"headway IS NOT NULL",
                 "pt_mode_bus"         :"mode ='bus'",
                 "pt_mode_tram"        :"mode ='tram'",
                 "pt_mode_train"       :"mode ='train'",
                 "pt_mode_ferry"       :"mode ='ferry'",
                 "pt_h60min"           :"headway <=60",
                 "pt_h30min"           :"headway <=30",
                 "pt_h25min"           :"headway <=25",
                 "pt_h20min"           :"headway <=20",
                 "pt_h10min"           :"headway <=10",
                 "pt_mode_bus_h30min"  :"mode = 'bus' AND headway <=30",
                 "pt_mode_train_h15min":"mode = 'train' AND headway <=15"}

for destination in pt_points.destination:
    ind_pt_table = 'ind_pt_d_800m_cl_{}'.format(destination)
    ind_headway_table = 'ind_pt_headway_800m_{}'.format(destination)
    result_table = 'od_pt_800m_cl_{}'.format(destination)
    print(" - {ind_pt_table}".format(ind_pt_table = ind_pt_table))

    # Create PT measures if not existing

    # Construct SQL queries to return minimum distance for each query, where met
    # we do this using the sorted dictionary, formatted as a list, so as to 
    # retain sensible column order in final output table
    queries = ',\n'.join(['MIN(CASE WHEN {} THEN distance END) {}'.format(q[1],q[0]) for q in list(sorted(pt_of_interest.items()))])

    sql = '''
    DROP TABLE IF EXISTS ind_point.{ind_pt_table};
    CREATE TABLE IF NOT EXISTS ind_point.{ind_pt_table} AS
    SELECT
    {points_id},
    {queries}
    FROM {sample_point_feature} p
    LEFT JOIN
        (SELECT {points_id},
                (obj->>'dest_oid')::int AS dest_oid,
                (obj->>'distance')::int AS distance,
                headway,
                mode
        FROM ind_point.{result_table},
            jsonb_array_elements(attributes) obj
        LEFT JOIN destinations.{destination} pt ON (obj->>'dest_oid')::int = pt.dest_oid
        WHERE attributes!='{curly_o}{curly_c}'::jsonb
        ) o USING ({points_id})
    GROUP BY {points_id};
    CREATE UNIQUE INDEX {ind_pt_table}_idx ON ind_point.{ind_pt_table} ({points_id});
    '''.format(points_id = points_id, 
               sample_point_feature=sample_point_feature,
               ind_pt_table = ind_pt_table,
               result_table=result_table,
               queries = queries,
               curly_o = '{',
               curly_c = '}',
               destination = destination)
    engine.execute(sql)
    
    print(" - {ind_headway_table}".format(ind_headway_table = ind_headway_table))
    #  The formula for effective headway within 800m is based on
    #  http://ngtsip.pbworks.com/w/page/12503387/Headway%20-%20Frequency
    #  supplied by Chris de Gruyter, and which presented formula
    #  SUM(60/headway)/60 
    #  however, this formula does not result in the estimate they present for
    #  their example
    #  "(60 minutes / 10 minute headway) + (60 minutes / 7 minute headway) + (60 minutes /5 minute headway)
    #     = 26.6 buses/hour ... 26.6 buses hour / 60 minutes = 2.25 effective headway"
    #  to achieve an effective headway of 2.25 from these values, you must do, 
    #  60/26.6 = 2.25, ie. NOT 26.6/60 , which = 0.44
    #  Also note that this is a rate, and the value '60' could just as easily be '720' as '1'
    #  The following are all equal to 2.25806451612903225808 , or 2.26
    #  (the difference from 2.25 is due to rounding error in the published formula's initial sum)
    #  SELECT 60/(60/10.0 + 60/7.0 + 60/5.0)  ;
    #  SELECT 720/(720/10.0 + 720/7.0 + 720/5.0)  ;
    #  SELECT 1/(1/10.0 + 1/7.0 + 1/5.0)      ;
    #  For simplicity, we present this in its reduced form '1'
    #  So, the required result is achieved using the method below
    
    # Additional note: Some stations in PT hubs at certain times (e.g. peak our in Sydney CBD) 
    # may approach a headway of zero --- and when rounding to nearest minute, this could actually
    # appear like it has been achieved.
    # For example, this stop has multiple outbound departures per minute in peak hour:
    # https://transitfeeds.com/p/transport-for-nsw/237/latest/stop/200055/20181203
    # it has a recorded peak hour headway of zero; and that results in a division by zero 
    # error when employing this formula.  As such, in the case of headway of zero, we must
    # replace with another number - and so we will use 0.5, which reflects multiple departures 
    # per minute in a meaningfully equivalent way.
    
    sql = '''
    DROP TABLE IF EXISTS ind_point.{ind_headway_table};
    CREATE TABLE IF NOT EXISTS ind_point.{ind_headway_table} AS
    SELECT
    {points_id},
    COUNT(*) stops_800m,
    MIN(headway) min_headway_800m,
    MAX(headway) max_headway_800m,
    AVG(headway) mean_headway_800m,
    stddev_pop(headway) sd_headway_800m,
    1/SUM(1/headway) effective_headway_800m
    FROM {sample_point_feature} p
    LEFT JOIN
    (SELECT {points_id},
            (obj->>'dest_oid')::int AS dest_oid,
            (obj->>'distance')::int AS distance,
            -- must avoid division by zero error for outlying stops with 
            -- multiple departures per minute
            CASE headway WHEN 0 THEN 0.5 ELSE headway END AS headway,
            mode
      FROM ind_point.{result_table},
           jsonb_array_elements(attributes) obj
      LEFT JOIN destinations.{destination} pt ON (obj->>'dest_oid')::int = pt.dest_oid
      WHERE attributes!='{curly_o}{curly_c}'::jsonb
      ) o USING ({points_id})
    WHERE distance <= 800
    GROUP BY {points_id};
    CREATE UNIQUE INDEX {ind_headway_table}_idx ON ind_point.{ind_headway_table} ({points_id});
    '''.format(points_id = points_id, 
            sample_point_feature=sample_point_feature,
            ind_headway_table = ind_headway_table,
            result_table=result_table,
            queries = queries,
            curly_o = '{',
            curly_c = '}',
            destination = destination)    
    # print(sql)
    engine.execute(sql)

# output to completion log    
script_running_log(script, task, start, locale)
engine.dispose()
