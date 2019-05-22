# Script:  _diagnostics.py
# Purpose: Outputs summary data (ind_summary) genereated by script 21 (parcel indicators) to Excel file
#          Supply name of person responsible to iterate over their study regions on this computer.
# Author:  Carl Higgs
# Date:    20190521

import os
import sys
import time
import pandas
from sqlalchemy import create_engine

date_time = time.strftime("%Y%m%d-%H%M")

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

who = sys.argv[1]
locales = responsible[responsible == who].sort_values().index.values.tolist()
outfile = '../data/study_region/indicator_summary_{}_{}.xlsx'.format(who,date_time)
print('''
Exporting to Excel file in location:
    {}
'''.format(outfile)),
with pandas.ExcelWriter(outfile) as writer:
    for locale in locales:
        full_locale = df_studyregion.loc[locale]['full_locale'].encode('utf')
        print('\n      - {}'.format(full_locale)),
        db = 'li_{}_{}'.format(locale,year)
        engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                     pwd  = db_pwd,
                                                                     host = db_host,
                                                                     db   = db))
        df = pandas.read_sql_query('''SELECT '' AS locale,
                                             '' AS year,
                                             '' AS subset,
                                             '' AS database,
                                             '' AS analyst,
                                             indicators AS variable,
                                             unit_level_description, 
                                             mean, 
                                             sd, 
                                             min, 
                                             max, 
                                             ROUND(null_pct,2) AS null_pct, 
                                             ROUND(100 - null_pct,2) AS complete_pct,
                                             count AS subset_count, 
                                             ROUND(count_pct,2) AS subset_pct
                                        FROM ind_summary
                                        LIMIT 0;
                                    ''', 
                                        con=engine)                                                         
        for subset in ['','urban','not_urban']:
            prefix = subset
            if subset == '':
                prefix = 'overall'
            else:
                subset = '_{}'.format(subset)
            df = df.append(pandas.read_sql_query('''SELECT '{}' AS locale,
                                                '{}' AS year,
                                                '{}' AS subset,
                                                '{}' AS database,
                                                '{}' AS analyst,
                                                indicators AS variable,
                                                unit_level_description, 
                                                mean, 
                                                sd, 
                                                min, 
                                                max, 
                                                ROUND(null_pct,2) AS null_pct, 
                                                ROUND(100 - null_pct,2) AS complete_pct
                                                count AS subset_count, 
                                                ROUND(count_pct,2) AS subset_pct
                                        FROM ind_summary{};
                                    '''.format(full_locale, year, prefix, db, who,subset), 
                                        con=engine))
            print("."),
        
        df.to_excel(writer,sheet_name='{}_{}'.format(locale,year), index=False)   

# TO DO add in dest counts summary