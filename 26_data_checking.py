# Script:  22_data_checking.py
# Purpose: Create data checking summary tables
# Author:  Carl Higgs 
# Date:    20190530

import datetime
import time
import psycopg2 
import numpy as np
from sqlalchemy import create_engine
from psycopg2.extensions import register_adapter, AsIs
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create data checking tables (for use in diagnostics script)'

# Connect to postgresql database     
conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))
                                                                 
# read in indicator matrix, really just to get the full names; but first we need to expand fully

ind_destinations = df_destinations[(df_destinations.locale == "*") | (df_destinations.locale == locale)].copy()
ind_destinations['destination'] = ind_destinations['destination'].apply(lambda x: "dist_m_{}".format(x))
ind_destinations = ind_destinations.set_index('destination')
ind_destinations.index.name = 'indicators'
ind_destinations = ind_destinations.loc[:,'unit_level_description':]

# Indicator configuration sheet is 'df_inds', read in from config file in the config script
# Restrict to indicators associated with study region (except distance to closest dest indicators)
ind_matrix = df_inds[df_inds['locale'].str.contains('|'.join([locale,'\*']))].copy().query('scale=="point"')

# # get the set of distance to closest regions which match for this region
# destinations = df_inds[df_inds['ind'].str.contains('destinations')]
# current_categories = [x for x in categories if 'distance_m_{}'.format(x) in destinations.ind_plain.str.encode('utf8').tolist()]
# ind_matrix = ind_matrix.append(destinations[destinations['ind_plain'].str.replace('distance_m_','').str.contains('|'.join(current_categories))])
ind_matrix['order'] = ind_matrix.index
ind_soft = ind_matrix.loc[ind_matrix.tags=='_{threshold}',:].copy()
ind_hard = ind_matrix.loc[ind_matrix.tags=='_{threshold}',:].copy()
ind_soft.replace(to_replace='{threshold}', value='soft', inplace=True,regex=True)
ind_hard.replace(to_replace='{threshold}', value='hard', inplace=True,regex=True)

ind_matrix = pandas.concat([ind_matrix,ind_soft,ind_hard], ignore_index=True).sort_values('ind')
ind_matrix.drop(ind_matrix[ind_matrix.tags == '_{threshold}'].index, inplace=True)
# Restrict to indicators with a defined query
ind_matrix = ind_matrix[pandas.notnull(ind_matrix['Query'])]
ind_matrix = ind_matrix[pandas.notnull(ind_matrix['updated?'])]

# Make concatenated indicator and tag name (e.g. 'walk_14' + 'hard')
# Tags could be useful later as can allow to search by name for e.g. threshold type,
# or other keywords (policy, binary, obsolete, planned --- i don't know, whatever)
# These tags are tacked on the end of the ind name seperated with underscores
ind_matrix['indicators'] = ind_matrix['ind'] + ind_matrix['tags'].fillna('')
# ind_matrix['sort_cat'] = pandas.Categorical(ind_matrix['ind'], categories=mylist, ordered=True)
# ind_matrix.sort_values('sort_cat', inplace=True)
# Compile list of indicators
ind_matrix.sort_values('order', inplace=True)

# Create an indicators summary table
ind_matrix = ind_matrix.set_index('indicators')
ind_matrix = ind_matrix.append(ind_destinations)
                                                                                                                                
df = pandas.read_sql_query('''SELECT * FROM parcel_indicators p LEFT JOIN dest_closest_indicators d ON p.gnaf_pid = d.gnaf_pid;''',con=engine)
new_cols = ['locale','summary_date','subset']
summary_urban = df.query("sos_name_2016 in ['Major Urban','Other Urban']").describe(include='all').transpose()
old_cols = summary_urban.columns
new_order = new_cols+list(old_cols)
summary_urban['locale'] = locale
summary_urban['summary_date'] = datetime.datetime.now().isoformat()
summary_urban['subset'] = 'Urban'
summary_urban = summary_urban[new_order]

summary_not_urban  = df.query("sos_name_2016 not in ['Major Urban','Other Urban']").describe(include='all').transpose()
summary_not_urban ['locale'] = locale
summary_not_urban ['summary_date'] = datetime.datetime.now().isoformat()
summary_not_urban ['subset'] = 'Not urban'
summary_not_urban = summary_not_urban[new_order]

full_summary = summary_urban.append(summary_not_urban)
full_summary.columns = [x.replace('%','_pct') for x in full_summary.columns]
new_order =  [x.replace('%','_pct') for x in new_order]
full_summary = full_summary.join(ind_matrix['unit_level_description'], how='left')
full_summary = full_summary[['unit_level_description']+new_order]
full_summary.to_sql('ind_summary',engine, if_exists='replace')

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
