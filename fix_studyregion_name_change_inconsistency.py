# Script:  fix_studyregion_name_change_inconsistency.py
# Purpose: Updating study region boundary names to generic schema
# Author:  Carl Higgs
# Date:    20190723

import os
import sys
import time
import pandas
import psycopg2

date_time = time.strftime("%Y%m%d-%H%M")

# Load settings from ind_study_region_matrix.xlsx
xls = pandas.ExcelFile(os.path.join(sys.path[0],'ind_study_region_matrix.xlsx'))
df_about = pandas.read_excel(xls, 'about')
print(df_about.iloc[0].reset_index()['index'].to_string(index=False).encode('utf'))
df_parameters = pandas.read_excel(xls, 'parameters',index_col=0)

df_parameters.value = df_parameters.value.fillna('')
for var in [x for x in df_parameters.index.values]:
    globals()[var] = df_parameters.loc[var]['value']    

df_studyregion = pandas.read_excel(xls, 'study_regions',index_col=1)
responsible = df_studyregion['responsible']


if len(sys.argv) < 2:
    sys.exit('''
This script requires an argument representing either an analyst, a single locale or a space-delimited set of locales.
For example:
   python _diagnostics.py Carl
   python _diagnostics.py albury_wodonga
   python _diagnostics.py albury_wodonga ballarat cairns launceston newcastle_maitland perth adelaide
''')

who = sys.argv[1]
if who in set(responsible.values):
    locales = responsible[responsible == who].sort_values().index.values.tolist()
elif who in responsible.sort_values().index.values:
    locales = sys.argv[1:]
    who = '_'.join(locales)
else:
    sys.exit('''
    The supplied command argument '{}' does not appear to correspond to either an analyst, a locale or list of locales.  Please check and try again.
    '''.format(who))
    
outfile = '{}study_region/indicator_summary_{}_{}.xlsx'.format(folderPath,who,date_time)
print('Updating study region boundary names to generic schema...'.format(outfile))


for locale in locales:
    full_locale = df_studyregion.loc[locale]['full_locale'].encode('utf')
    print('\n      - {}'.format(full_locale)),
    db = 'li_{}_{}'.format(locale,year)
    conn = psycopg2.connect(database=db, user=db_user, password=db_pwd, host=db_host,port=db_port)
    curs = conn.cursor()
    sql = '''                                                                                                               
        ALTER TABLE IF EXISTS gccsa_2018_10000m                      RENAME TO study_region_10000m                     ;
        ALTER TABLE IF EXISTS gccsa_2018_hex_3000m_diag              RENAME TO study_region_hex_3000m_diag             ;
        ALTER TABLE IF EXISTS gccsa_2018_hex_3000m_diag_3000m_buffer RENAME TO study_region_hex_3000m_diag_3000m_buffer;
        ALTER TABLE IF EXISTS sua_2018_10000m                        RENAME TO study_region_10000m                     ;
        ALTER TABLE IF EXISTS sua_2018_hex_3000m_diag                RENAME TO study_region_hex_3000m_diag             ;
        ALTER TABLE IF EXISTS sua_2018_hex_3000m_diag_3000m_buffer   RENAME TO study_region_hex_3000m_diag_3000m_buffer;
        ALTER TABLE IF EXISTS lga_2018_10000m                        RENAME TO study_region_10000m                     ;
        ALTER TABLE IF EXISTS lga_2018_hex_3000m_diag                RENAME TO study_region_hex_3000m_diag             ;
        ALTER TABLE IF EXISTS lga_2018_hex_3000m_diag_3000m_buffer   RENAME TO study_region_hex_3000m_diag_3000m_buffer;
        ALTER TABLE IF EXISTS study_region_2018_10000m                        RENAME TO study_region_10000m                     ;
        ALTER TABLE IF EXISTS study_region_2018_hex_3000m_diag                RENAME TO study_region_hex_3000m_diag             ;
        ALTER TABLE IF EXISTS study_region_2018_hex_3000m_diag_3000m_buffer   RENAME TO study_region_hex_3000m_diag_3000m_buffer;
    '''
    curs.execute(sql)
    conn.commit()

