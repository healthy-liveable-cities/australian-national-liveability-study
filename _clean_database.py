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
        DROP TABLE IF EXISTS abs_linkage;
        DROP TABLE IF EXISTS adoption;
        DROP TABLE IF EXISTS adoption_objectid_1_seq;
        DROP TABLE IF EXISTS alcohol_offlicence_act_2017;
        DROP TABLE IF EXISTS alcohol_offlicence_act_2017_objectid_seq;
        DROP TABLE IF EXISTS alcohol_offlicence_nt_2017;
        DROP TABLE IF EXISTS alcohol_offlicence_nt_2017_objectid_seq;
        DROP TABLE IF EXISTS alcohol_offlicence_qld_2017;
        DROP TABLE IF EXISTS alcohol_offlicence_qld_2017_objectid_12_seq;
        DROP TABLE IF EXISTS alcohol_offlicence_tas_2018;
        DROP TABLE IF EXISTS alcohol_offlicence_tas_2018_objectid_1_seq;
        DROP TABLE IF EXISTS alcohol_offlicence_wa_2017;
        DROP TABLE IF EXISTS alcohol_offlicence_wa_2017_objectid_1_seq;
        DROP TABLE IF EXISTS alcohol_onlicence_act_2017;
        DROP TABLE IF EXISTS alcohol_onlicence_act_2017_objectid_seq;
        DROP TABLE IF EXISTS alcohol_onlicence_nt_2017;
        DROP TABLE IF EXISTS alcohol_onlicence_nt_2017_objectid_seq;
        DROP TABLE IF EXISTS alcohol_onlicence_qld_2017;
        DROP TABLE IF EXISTS alcohol_onlicence_qld_2017_objectid_12_seq;
        DROP TABLE IF EXISTS alcohol_onlicence_tas_2018;
        DROP TABLE IF EXISTS alcohol_onlicence_tas_2018_objectid_1_seq;
        DROP TABLE IF EXISTS alcohol_onlicence_wa_2017;
        DROP TABLE IF EXISTS alcohol_onlicence_wa_2017_objectid_seq;
        DROP TABLE IF EXISTS aos_nodes_20m_line;
        DROP TABLE IF EXISTS aos_nodes_50m_line;
        DROP TABLE IF EXISTS compare_two_ncpf;
        DROP TABLE IF EXISTS dest_distance_m_old;
        DROP TABLE IF EXISTS dest_distances_3200m_old;
        DROP TABLE IF EXISTS educationlearning_primaryeducation;
        DROP TABLE IF EXISTS educationlearning_primaryeducation_objectid_1_seq;
        DROP TABLE IF EXISTS hospitalpharmacy;
        DROP TABLE IF EXISTS hospitalpharmacy_objectid_1_seq;
        DROP TABLE IF EXISTS islands;
        DROP TABLE IF EXISTS join_output;
        DROP TABLE IF EXISTS join_output_2;
        DROP TABLE IF EXISTS join_output_2_objectid_12_seq;
        DROP TABLE IF EXISTS join_output_objectid_1_seq;
        DROP TABLE IF EXISTS lga_ind_dwelling;
        DROP TABLE IF EXISTS lga_ind_person;
        DROP TABLE IF EXISTS main_lga_2016_aust;
        DROP TABLE IF EXISTS main_lga_2016_aust_ogc_fid_seq;
        DROP TABLE IF EXISTS main_mb_2016_aust_full;
        DROP TABLE IF EXISTS main_mb_2016_aust_full_ogc_fid_seq;
        DROP TABLE IF EXISTS main_sa1_2016_aust_full;
        DROP TABLE IF EXISTS main_sa1_2016_aust_full_ogc_fid_seq;
        DROP TABLE IF EXISTS main_sa2_2016_aust_full;
        DROP TABLE IF EXISTS main_sa2_2016_aust_full_ogc_fid_seq;
        DROP TABLE IF EXISTS main_sa3_2016_aust_full;
        DROP TABLE IF EXISTS main_sa3_2016_aust_full_ogc_fid_seq;
        DROP TABLE IF EXISTS main_sa4_2016_aust_full;
        DROP TABLE IF EXISTS main_sa4_2016_aust_full_ogc_fid_seq;
        DROP TABLE IF EXISTS main_sos_2016_aust;
        DROP TABLE IF EXISTS main_sos_2016_aust_ogc_fid_seq;
        DROP TABLE IF EXISTS main_ssc_2016_aust;
        DROP TABLE IF EXISTS main_ssc_2016_aust_ogc_fid_seq;
        DROP TABLE IF EXISTS mb_ind_dwelling;
        DROP TABLE IF EXISTS mb_ind_person;
        DROP TABLE IF EXISTS nhsd_2017_childdev_school_nursing;
        DROP TABLE IF EXISTS nhsd_2017_childdev_school_nursing_objectid_seq;
        DROP TABLE IF EXISTS non_abs_linkage;
        DROP TABLE IF EXISTS nrmr_2016_aust;
        DROP TABLE IF EXISTS nrmr_2016_aust_primaryindex_seq;
        DROP TABLE IF EXISTS od_aos_incorrect;
        DROP TABLE IF EXISTS od_aos_incorrect_geom;
        DROP TABLE IF EXISTS od_aos_jsonb_incorrect;
        DROP TABLE IF EXISTS od_aos_progress_incorrect;
        DROP TABLE IF EXISTS od_aos_test;
        DROP TABLE IF EXISTS od_aos_test2;
        DROP TABLE IF EXISTS od_aos_test2_geom;
        DROP TABLE IF EXISTS od_aos_test_geom;
        DROP TABLE IF EXISTS region_ind_dwelling;
        DROP TABLE IF EXISTS region_ind_person;
        DROP TABLE IF EXISTS sa2_ind_dwelling;
        DROP TABLE IF EXISTS sa2_ind_person;
        DROP TABLE IF EXISTS sa3_ind_dwelling;
        DROP TABLE IF EXISTS sa3_ind_person;
        DROP TABLE IF EXISTS sa4_ind_dwelling;
        DROP TABLE IF EXISTS sa4_ind_person;
        DROP TABLE IF EXISTS sc_nh1600m_15m;
        DROP TABLE IF EXISTS sos_ind_dwelling;
        DROP TABLE IF EXISTS sos_ind_person;
        DROP TABLE IF EXISTS ssc_ind_dwelling;
        DROP TABLE IF EXISTS ssc_ind_person;
        DROP TABLE IF EXISTS study_region_2016_aust;
        DROP TABLE IF EXISTS gccsa_{year};
        DROP TABLE IF EXISTS sua_{year};
    '''.format(locale = locale, year = year)
    curs.execute(sql)
    conn.commit()

    # ArcGIS environment settings
    arcpy.env.workspace = gdb_path  
    arcpy.Rename_management('gccsa_2018_10000m'                      ,'study_region_10000m'                     )
    arcpy.Rename_management('gccsa_2018_hex_3000m_diag'              ,'study_region_hex_3000m_diag'             )
    arcpy.Rename_management('gccsa_2018_hex_3000m_diag_3000m_buffer' ,'study_region_hex_3000m_diag_3000m_buffer')
    arcpy.Rename_management('sua_2018_10000m'                        ,'study_region_10000m'                     )
    arcpy.Rename_management('sua_2018_hex_3000m_diag'                ,'study_region_hex_3000m_diag'             )
    arcpy.Rename_management('sua_2018_hex_3000m_diag_3000m_buffer'   ,'study_region_hex_3000m_diag_3000m_buffer')
    arcpy.Rename_management('lga_2018_10000m'                        ,'study_region_10000m'                     )
    arcpy.Rename_management('lga_2018_hex_3000m_diag'                ,'study_region_hex_3000m_diag'             )
    arcpy.Rename_management('lga_2018_hex_3000m_diag_3000m_buffer'   ,'study_region_hex_3000m_diag_3000m_buffer')