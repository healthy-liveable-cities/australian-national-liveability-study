# Script:  fix_studyregion_name_change_inconsistency.py
# Purpose: Updating study region boundary names to generic schema
# Author:  Carl Higgs
# Date:    20190723

import os
import sys
import time
import pandas
import psycopg2
import arcpy

date_time = time.strftime("%Y%m%d-%H%M")

# Load settings from ind_study_region_matrix.xlsx
xls = pandas.ExcelFile(os.path.join(sys.path[0],'ind_study_region_matrix.xlsx'))
df_about = pandas.read_excel(xls, 'about')
print(df_about.iloc[0].reset_index()['index'].to_string(index=False).encode('utf'))
df_parameters = pandas.read_excel(xls, 'parameters',index_col=0)

df_parameters.value = df_parameters.value.fillna('')
for var in [x for x in df_parameters.index.values]:
    globals()[var] = df_parameters.loc[var]['value']    
df_housekeeping = pandas.read_excel(xls, 'housekeeping')
purge_table_list = list(set(df_housekeeping.tables_to_drop_if_exist.tolist()))

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
    locale_dir = os.path.join(folderPath,'study_region','{}'.format(locale.lower()))
    gdb = '{}.gdb'.format(db)
    gdb_path = os.path.join(locale_dir,gdb)
    conn = psycopg2.connect(database=db, user=db_user, password=db_pwd, host=db_host,port=db_port)
    curs = conn.cursor()
    sql = '''                                                                                                               
        ALTER TABLE IF EXISTS gccsa_2018_10000m                               RENAME TO study_region_10000m                     ;
        ALTER TABLE IF EXISTS gccsa_2018_hex_3000m_diag                       RENAME TO study_region_hex_3000m_diag             ;
        ALTER TABLE IF EXISTS gccsa_2018_hex_3000m_diag_3000m_buffer          RENAME TO study_region_hex_3000m_diag_3000m_buffer;
        ALTER TABLE IF EXISTS sua_2018_10000m                                 RENAME TO study_region_10000m                     ;
        ALTER TABLE IF EXISTS sua_2018_hex_3000m_diag                         RENAME TO study_region_hex_3000m_diag             ;
        ALTER TABLE IF EXISTS sua_2018_hex_3000m_diag_3000m_buffer            RENAME TO study_region_hex_3000m_diag_3000m_buffer;
        ALTER TABLE IF EXISTS lga_2018_10000m                                 RENAME TO study_region_10000m                     ;
        ALTER TABLE IF EXISTS lga_2018_hex_3000m_diag                         RENAME TO study_region_hex_3000m_diag             ;
        ALTER TABLE IF EXISTS lga_2018_hex_3000m_diag_3000m_buffer            RENAME TO study_region_hex_3000m_diag_3000m_buffer;
        ALTER TABLE IF EXISTS study_region_2018_10000m                        RENAME TO study_region_10000m                     ;
        ALTER TABLE IF EXISTS study_region_2018_hex_3000m_diag                RENAME TO study_region_hex_3000m_diag             ;
        ALTER TABLE IF EXISTS study_region_2018_hex_3000m_diag_3000m_buffer   RENAME TO study_region_hex_3000m_diag_3000m_buffer;
        '''
    curs.execute(sql)
    conn.commit()
    # ArcGIS environment settings
    arcpy.env.workspace = gdb_path  
    to_rename = [['gccsa_2018_10000m'                      ,'study_region_10000m'                     ],
                 ['gccsa_2018_hex_3000m_diag'              ,'study_region_hex_3000m_diag'             ],
                 ['gccsa_2018_hex_3000m_diag_3000m_buffer' ,'study_region_hex_3000m_diag_3000m_buffer'],
                 ['sua_2018_10000m'                        ,'study_region_10000m'                     ],
                 ['sua_2018_hex_3000m_diag'                ,'study_region_hex_3000m_diag'             ],
                 ['sua_2018_hex_3000m_diag_3000m_buffer'   ,'study_region_hex_3000m_diag_3000m_buffer'],
                 ['lga_2018_10000m'                        ,'study_region_10000m'                     ],
                 ['lga_2018_hex_3000m_diag'                ,'study_region_hex_3000m_diag'             ],
                 ['lga_2018_hex_3000m_diag_3000m_buffer'   ,'study_region_hex_3000m_diag_3000m_buffer']]
    for feature in to_rename:
        try:
            old_name = feature[0]
            new_name = feature[1]
            arcpy.Rename_management(old_name,new_name)
            print("."),
        except:
            print("."),
    print("Done.")
    print("Remove redundant tables (see 'housekeeping' in config file) if they exist... "),
    for table in purge_table_list:
       sql = "DROP TABLE IF EXISTS {}".format(table)
       curs.execute(sql)
       conn.commit()
    print("Done.")
