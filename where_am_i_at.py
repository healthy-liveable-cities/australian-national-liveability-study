# Script:  who.py
# Purpose: who is processing which study regions?
# Author:  Carl Higgs
# Date:    20181009

# Import custom variables for National Liveability indicator process
# import modules
import os
import sys
import pandas
from sqlalchemy import create_engine

# Load settings from ind_study_region_matrix.xlsx
xls = pandas.ExcelFile(os.path.join(sys.path[0],'ind_study_region_matrix.xlsx'))
df_about = pandas.read_excel(xls, 'about')
print(df_about.iloc[0].reset_index()['index'].to_string(index=False).encode('utf'))
df_parameters = pandas.read_excel(xls, 'parameters',index_col=0)
df_studyregion = pandas.read_excel(xls, 'study_regions',index_col=1)
responsible = df_studyregion['responsible']
year   = df_parameters.loc['year']['value']
# SQL Settings
db_host   = df_parameters.loc['db_host']['value'].encode('utf')
db_port   = '{}'.format(df_parameters.loc['db_port']['value'])
db_user   = df_parameters.loc['db_user']['value'].encode('utf')
db_pwd    = df_parameters.loc['db_pwd']['value'].encode('utf')

locales = responsible[responsible == sys.argv[1]].index.values.tolist()
for locale in locales:
  print('\nScripts processed for {}:'.format(locale))
  db = 'li_{}_{}'.format(locale,year)
  engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))
  status = pandas.read_sql("SELECT script,datetime_completed FROM script_log", engine)
  print(status)
  