# Script:  _project_setup.py
# Liveability indicator calculation template custom configuration file
# Version: 20180907
# Author:  Carl Higgs
#
# All scripts within the process folder draw on the sources, parameters and modules
# specified in the file _project_configuration.xlsx to source and output 
# resources. It is the best definition of where resources are sourced from and 
# how the methods used have been parameterised.
#
# If you are starting a new project, you can set up the global parameters which 
# (pending overrides) should be applied for each study region in the 
#  detailed_explanation' folder.
#
# If you are adding a new study region to an existing project, this study region
# will be entered as a row in the 'study_regions' worksheet; the corresponding
# column fields must be completed as required.  See the worksheet 'detailed 
# explanation' for a description of what is expected for each field.
#
# If you are running a project on a specific computer that requires some kind of 
# override of the parameters set up above, you can **in theory** use the 
# 'local_environments' worksheet to do this.  In practice this hasn't been 
# implemented yet, and the sheet is just a placeholder for the event that such 
# overrides are required.
#
# The file which draws on the project, study region, destination and local settings 
# specificied in the _project_configuration.xlsx file and implements these across 
# scripts is THIS FILE _project_setup.py

# import modules
import os
import sys
import time
import pandas
import numpy as np
import subprocess as sp

# Load settings from _project_configuration.xlsx
xls = pandas.ExcelFile(os.path.join(sys.path[0],'_project_configuration.xlsx'))
df_parameters = pandas.read_excel(xls, 'parameters',index_col=0)
df_regions = pandas.read_excel(xls, 'regions',index_col=0)
df_studyregion = pandas.read_excel(xls, 'study_regions',index_col=1)
df_inds = pandas.read_excel(xls, 'indicators')
df_destinations = pandas.read_excel(xls, 'destinations')
df_osm = pandas.read_excel(xls, 'osm_and_open_space_defs')
df_osm_dest = pandas.read_excel(xls, 'osm_dest_definitions')
df_data_catalogue = pandas.read_excel(xls, 'data_catalogue')
df_housekeeping = pandas.read_excel(xls, 'housekeeping')

df_highlife_indicators = pandas.read_excel(xls, 'highlife_indicators')

df_parameters.value = df_parameters.value.fillna('')
for var in [x for x in df_parameters.index.values]:
    globals()[var] = df_parameters.loc[var]['value']    

study_regions = [x.encode() for x in df_studyregion.index.tolist() if x not in ['testing','australia']]
responsible = df_studyregion['responsible']

# The main directory for data

# Set up locale (ie. defined at command line, or else testing)
if len(sys.argv) >= 2:
  locale = '{studyregion}'.format(studyregion = sys.argv[1])
else:
  locale = test_region
if __name__ == '__main__':
  print("\nProcessing script {} for locale {}...\n".format(sys.argv[0],locale))

  
# More study region details
full_locale = df_studyregion.loc[locale]['full_locale']
region = df_studyregion.loc[locale]['region']
state  = df_studyregion.loc[locale]['state']

locale_dir = os.path.join(folderPath,'study_region','{}'.format(locale.lower()))

# Study region boundary
region_shape = df_studyregion.loc[locale]['region_shape']

# SQL Query to select study region
region_where_clause = df_studyregion.loc[locale]['region_where_clause']

# db suffix
suffix = df_studyregion.loc[locale]['suffix']
if suffix.dtype!='float64':
  # this implies at least one value was a string, and this can be encoded as utf
  suffix = suffix
  
if np.isnan(suffix):
  # this implies all suffixes are blank and this has been interpreted as 'nan'
  suffix = ''

# derived study region name (no need to change!)
study_region = 'study_region'
db = '{}_{}_{}{}'.format(project_prefix,locale,year,suffix).lower()

# Study region buffer
buffered_study_region = 'study_region_{}{}'.format(study_buffer,units)

# Derived hex settings - no need to change
poly_check = ['{}'.format(polygon_feature),'{}'.format(polygon_id),'{}'.format(polygon_unit)]
if True in [x in ['','nan'] for x in poly_check]:
    hex_grid = '{0}_hex_{1}{2}_diag'.format(study_region,hex_diag,units)
    hex_grid_buffer =  '{0}_hex_{1}{2}_diag_{3}{2}_buffer'.format(study_region,hex_diag,units,hex_buffer)
    hex_side = float(hex_diag)*0.5

# Database names -- derived from above parameters; (no need to change!)
gdb       = '{}.gdb'.format(db)
db_sde    = '{}.sde'.format(db)
gdb_path    = os.path.join(locale_dir,gdb)
db_sde_path = os.path.join(locale_dir,db_sde)
dbComment = 'Liveability indicator data for {0} {1}.'.format(locale,year)

os.environ['PGHOST']     = db_host
os.environ['PGPORT']     = str(db_port)
os.environ['PGUSER']     = db_user
os.environ['PGPASSWORD'] = db_pwd
os.environ['PGDATABASE'] = db

# Sample point feature name
sample_point_feature = '{}_accesspts_edited'.format(locale)

preprocessed_data = os.path.join(folderPath,'study_region',locale,preprocessed_data.format(locale = locale))

osm_data = os.path.join(df_studyregion.loc[locale]['osm_data'])
osm2pgsql_exe = os.path.join(folderPath,df_parameters.loc['osm2pgsql_exe']['value'])
osm2pgsql_style = os.path.join(folderPath,df_parameters.loc['osm2pgsql_style']['value'])
osm_source = df_studyregion.loc[locale]['osm_source']
osm_prefix = df_studyregion.loc[locale]['osm_prefix']

area_schemas = ['ind_{}'.format(x) for x in df_regions.query("purpose.str.contains('analysis')",engine='python').abbreviation.values]+['ind_region']
schemas = ['public',boundary_schema,network_schema,osm_schema,destinations_schema,open_space_schema,school_schema,point_schema,distance_schema,validation_schema,processing_schema]+area_schemas

users = [db_user, arc_sde_user]
def grant_schema_query(user,schema):
    query = '''
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {user};
        GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {user};
        '''.format(user=user, schema = schema)
    return query

grant_query = ''
for user in [db_user, arc_sde_user]:
    # grant_query = grant_query + '''
        # GRANT postgres TO {user};
        # '''.format(user=user)
    for schema in schemas:
        grant_query = grant_query + grant_schema_query(user,schema)
        

#Highlife region set up
# analysis_regions = ['footprints']

# Region set up
areas_of_interest = df_regions.index.values.tolist()
geographies = df_regions[df_regions['purpose'].str.contains('geo')==True].index.values.tolist() 
geo_imports = df_regions.loc[df_regions.loc[geographies,'epsg'].dropna().reset_index().name.dropna(),['data','epsg']].groupby(['data','epsg']).size().reset_index()
analysis_regions = df_regions[df_regions['purpose'].str.contains('analysis')==True].index.values.tolist()

csv_linkage = df_regions[df_regions['format'].str.contains('csv')==True].index.values.tolist()
areas = analysis_regions

# meshblock ID MB_CODE_20 (varname is truncated by arcgis to 8 chars) datatype is varchar(11) 
meshblock_id     = df_regions.iloc[0]['id']
dwellings        = df_regions.loc['Dwellings','data']
dwellings_id     = df_regions.loc['Dwellings','linkage_id']
dwellings_field  = df_regions.loc['Dwellings','id']


# roads
# Define network data name structures
network_source = os.path.join(locale_dir,df_studyregion.loc[locale]['network_folder'])
network_template = os.path.join(folderPath,df_parameters.loc['network_template']['value'])
in_network_dataset = os.path.join('{}'.format(network_source_feature_dataset),
                                '{}_ND'.format(network_source_feature_dataset))
# network dataset, with full path
in_network_dataset_path = os.path.join(gdb_path,in_network_dataset)

service_areas =  [int(x) for x in service_areas.split(',')]
service_areas = sorted(set(service_areas +[distance]))

# Island exceptions are defined using ABS constructs in the project configuration file.
# They identify contexts where null indicator values are expected to be legitimate due to true network isolation, 
# not connectivity errors. 
# For example, for Rottnest Island in Western Australia: sa1_maincode IN ('50702116525')
island_exception = df_studyregion.fillna('').loc[locale]['island_exception']

# Sausage buffer run parameters
# If you experience 'no forward edges' issues, change this value to 1
# this means that for *subsequently processed* buffers, it will use 
# an ST_SnapToGrid parameter of 0.01 instead of 0.001
## The first pass should use 0.001, however.
no_foward_edge_issues = df_studyregion.loc[locale]['no_forward_edge_issues']
snap_to_grid = 0.001
if no_foward_edge_issues == 1:
  snap_to_grid = 0.01
  
# Destinations
# Destinations data directory
dest_dir = os.path.join(folderPath,df_parameters.loc['dest_dir']['value'])
src_destinations = os.path.join(dest_dir,df_parameters.loc['src_destinations']['value'])

# when recompiling destinations, the attributes are copied to csv in case later linkage is req'data
# some fields are problematic however -- too large.  detail here to not copy.
# f.name
dest_drop_attributes_ftype = [u'Date']
dest_drop_attributes_fname = ['Conditions_on_Approval','Snippet']

# derived source destinations gdb location - no need to change
dest_gdb = os.path.join(folderPath,src_destinations)  
# This is the parcel destination OD matrix derived table 
# (used to be called 'dist_cl_od_parcel_dest' --- simplified to 'od_distances')
od_distances = "od_distances"

# I have created a feature we use as a template for a combined feature
combined_dest_template = os.path.join(folderPath,
                                      df_parameters.loc['combined_dest_template']['value'])
outCombinedFeature = 'study_destinations'

# array / list of destinations 
# IMPORTANT -- These are specified in the 'destinations' worksheet of the _project_configuration.xlsx file
#               - specify: destination, domain, cutoff and count distances as required
#
#           -- If new destinations are added, they should be appended to end of list 
#              to ensure this order is respected across time.
#
# The table 'dest_type' will be created in Postgresql to keep track of destinations

df_destinations = df_destinations.replace(np.nan, 'NULL', regex=True)
destination_list = [x for x in df_destinations.destination.tolist()] # the destinations 
# dest_codes = df_destinations.code.tolist()   # domain is an optional grouping category for destinations / indicators
# dest_domains = df_destinations.domain.tolist()   # domain is an optional grouping category for destinations / indicators
# dest_cutoffs = df_destinations.cutoff.tolist()   # cut off distance within which to evaluate presence
# dest_counts = df_destinations.counts.tolist()   # cut off distance within which to evaluate counts

df_osm_dest = df_osm_dest.replace(np.nan, 'NULL', regex=True)

school_table = os.path.splitext(os.path.basename(school_ratings))[0]
childcare_table = os.path.splitext(os.path.basename(childcare_ratings))[0]

# When destinations are imported for study region, we don't want to retain all of these; now, purge
purge_table_list = list(set(df_housekeeping.tables_to_drop_if_exist.tolist()))

def pretty(d, indent=0):
   for key, value in d.items():
      depth = 0
      print('\t' * indent + str(key)+':'),
      if isinstance(value, dict):
        if depth == 0:
          print(" ")
          depth+=1
        pretty(value, indent+1)
      else:
        print(' ' + str(value))

def indicator_summary_sql(indicator_tuples):
    ''' 
    given a list of indicators, return SQL code to summarise these
    '''
    # ensure input is in list form
    if not hasattr(indicator_tuples,"__iter__"):
        raise ValueError("The provided data is not in the expected form; ie. a tuple of ['indicator','scale','description']")
    summary_queries = []
    for ind in [i for i in indicator_tuples if i[1] !='NULL']:
        sql = '''
        {scale}*AVG("{ind}") AS "{ind}"
        '''.format(ind = ind[0],scale = ind[1])
        summary_queries.append(sql)
    return ','.join(summary_queries)


# specify that the above modules and all variables below are imported on 'from config.py import *'
__all__ = [x for x in dir() if x not in ['__file__','__all__', '__builtins__', '__doc__', '__name__', '__package__']]
 
