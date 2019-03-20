# Purpose: Purge childcare destinations from database
#
# Authors:  Carl Higgs
# Date:    20190319


import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import time
import psycopg2
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

category = 'childcare'
string     = 'childcare_%'
task = 'Purge {} destinations from dest_type, od_counts, od_closest, od_distances_3200m tables... '.format(category),
for table in ['dest_type','od_counts','od_closest','od_distances_3200m']:
    sql = '''
    DELETE FROM {table} WHERE dest_class LIKE '{string}';
    '''.format(table = table, string = string)
    curs.execute(sql)
    conn.commit()
print('Done.')

print('Purge redundant NHSD destinations, no longer in gdb, in case they have previously been processed... '),
destinations_to_purge = ['Adoption','AgedCare','AgedCareResidentialServices','AlliedHealth','AlternativeComplementaryTherapies','ChildCare_Kindergarten','ChildDevelopment','ChildProtection_FamilyServices','CommunityHealthCare','Counselling','CrisisLine','DeathServices','Dental','DisabilitySupport','Drug_Alcohol','Education_Learning','EducationLearning_PrimaryEducation','EmergencyDepartment','Employment','Financial_MaterialAid','GP_doctor','HomeSupportServices','HospitalPharmacy','Housing_Homelessness','Interpreting','Justice','Legal','MentalHealth','NHSD_Dec2017','PainManagementService','Regulation','Respite_CarerSupport','SpecialistClinicalPathology','SpecialistMedical','SpecialistObstetricsGynaecology','SpecialistPaediatric','SpecialistRadiology_Imaging','SpecialistSurgical','SupportGroup','Transport']

for dest in destinations_to_purge:
    for table in ['dest_type','od_counts','od_closest','od_distances_3200m']:
        sql = '''
        DELETE FROM {table} WHERE dest_class = '{string}';
        '''.format(table = table, string = dest)
        curs.execute(sql)
        conn.commit()
        print("."),
print("Done.")

old_name = 'ChildProtectionFamilyServices_IntegratedFamilyServices'
new_name = 'ChildProtectionFamilyServices_Integrated'
print("Ensure any results previously processed under (too long) name '{old}' have their dest_class variable renamed to '{new}'... ".format(old = old_name, new = new_name)),
for table in ['dest_type','od_counts','od_closest','od_distances_3200m']:
    sql = '''
    UPDATE {table} SET dest_class = '{new}' WHERE dest_class = '{old}'
    '''.format(table = table, old = old_name, new = new_name)
    curs.execute(sql)
    conn.commit()
for table in ['dest_type','od_closest']:
    sql = '''
    UPDATE {table} SET dest_class = '{new}', dest_name = '{new}' WHERE dest_class = '{old}'
    '''.format(table = table, old = old_name, new = new_name)
    curs.execute(sql)
    conn.commit()
print("Done.")

# output to completion log    
script_running_log(script, task, start, locale)
conn.close()
