# Script:  config_ntnl_li_process.py
# Liveability indicator calculation template custom configuration file
# Version: 20180907
# Author:  Carl Higgs
#
# All scripts within the process folder draw on the sources, parameters and modules
# specified in the file ind_study_region_matrix.xlsx to source and output 
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
# specificied in the ind_study_region_matrix.xlsx file and implements these across 
# scripts is THIS FILE config_ntnl_li_process.py

# import modules
import os
import sys
import time
import pandas
import subprocess as sp

# Load settings from ind_study_region_matrix.xlsx
xls = pandas.ExcelFile(os.path.join(sys.path[0],'ind_study_region_matrix.xlsx'))
df_parameters = pandas.read_excel(xls, 'parameters',index_col=0)
df_studyregion = pandas.read_excel(xls, 'study_regions',index_col=1)
df_inds = pandas.read_excel(xls, 'ind_study_region_matrix')
df_destinations = pandas.read_excel(xls, 'destinations')
df_osm = pandas.read_excel(xls, 'osm_and_open_space_defs')
df_osm_dest = pandas.read_excel(xls, 'osm_dest_definitions')
df_data_catalogue = pandas.read_excel(xls, 'data_catalogue')
df_housekeeping = pandas.read_excel(xls, 'housekeeping')

responsible = df_studyregion['responsible']

year   = df_parameters.loc['year']['value']

# The main directory for data
folderPath = df_parameters.loc['folderPath']['value']

# check current locale based on git branch status
# if current branch is master, the scripts will fail (locale = '')
# if current branch is 'li_studyregion_2016', locale will be set as 'studyregion'
locale = '_'.join(sp.check_output(["git", "status"],cwd=sys.path[0],shell=True).split('\n')[0].split(' ')[2].split('_')[1:-1])

if len(sys.argv) >= 2:
  locale = '{studyregion}'.format(studyregion = sys.argv[1])
else:
  if __name__ == '__main__':
    print("Note: locale has not been specified; attempting to use current git respository branch to glean study region...")
  if locale == '':
    locale = 'testing'
if __name__ == '__main__':
  print("\nProcessing script {} for locale {}...\n".format(sys.argv[0],locale))

# More study region details

region = df_studyregion.loc[locale]['region'].encode('utf')
state  = df_studyregion.loc[locale]['state'].encode('utf')

locale_dir = os.path.join(folderPath,'study_region','{}'.format(locale.lower()))

# Study region boundary
region_shape = os.path.join(folderPath,df_studyregion.loc[locale]['region_shape'])

# SQL Query to select study region
region_where_clause = df_studyregion.loc[locale]['region_where_clause'].encode('utf')

# db suffix
suffix = df_studyregion.loc[locale]['suffix']
if suffix.dtype!='float64':
  # this implies at least one value was a string, and this can be encoded as utf
  suffix = suffix.encode('utf')
  
if pandas.np.isnan(suffix):
  # this implies all suffixes are blank and this has been interpreted as 'nan'
  suffix = ''


# derived study region name (no need to change!)
study_region = '{0}_{1}'.format(region,year).lower()
db = 'li_{0}_{1}{2}'.format(locale,year,suffix).lower()

# ; Project spatial reference (for ArcGIS)
SpatialRef = df_parameters.loc['SpatialRef']['value'].encode('utf')

# Project spatial reference EPSG code (for Postgis)
srid       = df_parameters.loc['srid']['value']
units      = df_parameters.loc['units']['value'].encode('utf')
units_full = df_parameters.loc['units_full']['value'].encode('utf')

# Study region buffer
study_buffer = df_parameters.loc['study_buffer']['value']
buffered_study_region = '{0}_{1}{2}'.format(study_region,study_buffer,units)

# Number of processors to use in when multiprocessing
nWorkers = df_parameters.loc['multiprocessing']['value']

# hexagon diagonal length and buffer distance (metres)
#   -- hexagon sides will be half the length of this value
#   -- hexagon area is 3/2 * sqrt(3) * (hex_diag/2)^2
#  so with diag of 3000 m, area is 5845671.476 sq.m.
hex_diag   = df_parameters.loc['hex_diag']['value']
hex_buffer = df_parameters.loc['hex_buffer']['value']

# Derived hex settings - no need to change
hex_grid = '{0}_hex_{1}{2}_diag'.format(study_region,hex_diag,units)
hex_grid_buffer =  '{0}_hex_{1}{2}_diag_{3}{2}_buffer'.format(study_region,hex_diag,units,hex_buffer)
hex_side = float(hex_diag)*0.5

# Temp folder  - be aware that some files may be created and stored here; you may need to manually remove such files 
temp = df_parameters.loc['temp']['value'].encode('utf')


# location of the 'Create_Hexagon_Tessellation' user written package toolbox by Tim Whiteaker; acquired from http://www.arcgis.com/home/item.html?id=03388990d3274160afe240ac54763e57
create_hexagon_tbx = df_parameters.loc['create_hexagon_tbx']['value'].encode('utf')
CreatePointsLines_tbx = df_parameters.loc['CreatePointsLines_tbx']['value'].encode('utf')

# TRANSFORMATIONS
#  These three variables are used for specifying a transformation from GCS GDA 1994 to GDA2020 GA LLC when using arcpy.Project_management.  Specifically, its used in the custom clipFeature function in script 02_road_network_setup.py
out_coor_system = df_parameters.loc['out_coor_system']['value'].encode('utf')

transform_method = df_parameters.loc['transform_method']['value'].encode('utf')

in_coor_system = df_parameters.loc['in_coor_system']['value'].encode('utf')

## This is used for spatial reference for 'destinations' feature dataset (in script 07_recompile_destinations.py --- similar to out_coor_system, it contains additions bounding box parameters apparently, and a flag 'IsHighPrecision'.  
feature_ds_out_spatial_ref = df_parameters.loc['feature_ds_out_spatial_ref']['value']

# SQL Settings
db_host   = df_parameters.loc['db_host']['value'].encode('utf')
db_port   = '{}'.format(df_parameters.loc['db_port']['value'])
db_user   = df_parameters.loc['db_user']['value'].encode('utf')
db_pwd    = df_parameters.loc['db_pwd']['value'].encode('utf')
arc_sde_user = df_parameters.loc['arc_sde_user']['value'].encode('utf')

# Database names -- derived from above parameters; (no need to change!)
gdb       = '{}.gdb'.format(db)
db_sde    = '{}.sde'.format(db)
gdb_path    = os.path.join(locale_dir,gdb)
db_sde_path = os.path.join(locale_dir,db_sde)
dbComment = 'Liveability indicator data for {0} {1}.'.format(locale,year)


os.environ['PGHOST']     = db_host
os.environ['PGPORT']     = db_port
os.environ['PGUSER']     = db_user
os.environ['PGPASSWORD'] = db_pwd
os.environ['PGDATABASE'] = db

osm2pgsql_exe = os.path.join(folderPath,df_parameters.loc['osm2pgsql_exe']['value'].encode('utf'))
osm2pgsql_style = os.path.join(folderPath,df_parameters.loc['osm2pgsql_style']['value'].encode('utf'))
osm_source = df_studyregion.loc[locale]['osm_source'].encode('utf')
osm_prefix = df_studyregion.loc[locale]['osm_prefix'].encode('utf')

grant_query = '''GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {0};
                 GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {0};
                 GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {1};
                 GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {1};'''.format(arc_sde_user,db_user)

# ABS data
# ABS data sourced online is located in the ABS_downloads folder
# cleaned data referenced below (including output from scripts) is located in ABS_derived

# Index of Relative Socio-Economic Disadvantage (cleaned)
abs_irsd  = os.path.join(folderPath,
                         df_parameters.loc['abs_irsd']['value'].encode('utf'))
              
# meshblock source shape file (ABS download)
# Meshblock Dwellings feature name
meshblocks   = os.path.join(folderPath,
                         df_parameters.loc['meshblocks']['value'].encode('utf'))

# meshblock ID MB_CODE_20 (varname is truncated by arcgis to 8 chars) datatype is varchar(11) 
meshblock_id    = df_parameters.loc['meshblock_id']['value'].encode('utf')

# Dwelling count source csv (ABS download)
# CLEAN APPLIED: removed comments from end of file
dwellings        = os.path.join(folderPath,
                         df_parameters.loc['dwellings']['value'].encode('utf'))
dwellings_id     = df_parameters.loc['dwellings_id']['value'].encode('utf')
dwellings_field  = df_parameters.loc['dwellings_id']['value'].encode('utf')

# other areas
abs_SA1 = os.path.join(folderPath,
                      df_parameters.loc['abs_SA1']['value'].encode('utf'))
abs_SA2 = os.path.join(folderPath,              
                      df_parameters.loc['abs_SA2']['value'].encode('utf'))
abs_SA3 = os.path.join(folderPath,              
                      df_parameters.loc['abs_SA3']['value'].encode('utf'))
abs_SA4 = os.path.join(folderPath,              
                      df_parameters.loc['abs_SA4']['value'].encode('utf'))
abs_SOS = os.path.join(folderPath,
                      df_parameters.loc['abs_SOS']['value'].encode('utf'))
abs_lga = os.path.join(folderPath,              
                      df_parameters.loc['abs_lga']['value'].encode('utf'))
abs_suburb = os.path.join(folderPath,
                      df_parameters.loc['abs_suburb']['value'].encode('utf'))

# parcels (point data locations used for sampling)
# Note that the process assumes we have already transformed points to the project's spatial reference
# Point data locations (e.g. GNAF address point features)
points = df_studyregion.loc[locale]['points'].encode('utf')
points = points.split(',')
points_id = df_parameters.loc['points_id']['value'].encode('utf')
points_srid = df_parameters.loc['points_srid']['value']

# The below is perhaps a redundant naming convention,
# but our last run of scripts invested in this, so for now we'll leave it in so things work
# A better name might be something like 'units_of_analysis' or 'included_points'
# I don't know; but that is what this refers to. Its just a name.
parcel_dwellings = 'parcel_dwellings'

# roads
# Define network data name structures
road_data = df_parameters.loc['road_data']['value'].encode('utf')  # the folder where road data is kept
network_source = os.path.join(locale_dir,df_studyregion.loc[locale]['network_folder'].encode('utf'))
network_source_feature_dataset = df_parameters.loc['network_source_feature_dataset']['value'].encode('utf')
network_edges = df_parameters.loc['network_edges']['value'].encode('utf')
network_junctions = df_parameters.loc['network_junctions']['value'].encode('utf')
network_template = os.path.join(folderPath,road_data,df_parameters.loc['network_template']['value'].encode('utf'))

# transformations for network (currently WGS84 to GDA GA LCC using NTv2)
network_transform_method = df_parameters.loc['network_transform_method']['value'].encode('utf')
network_in_coor_system   = df_parameters.loc['network_in_coor_system']['value'].encode('utf')

# Intersections with 3plus ways
clean_intersections_gpkg = df_parameters.loc['clean_intersections_gpkg']['value'].encode('utf')
clean_intersections_locale = df_studyregion.loc[locale]['clean_intersections_locale'].encode('utf')
# intersections = os.path.join(folderPath,'roads/GDA2020_GA_LCC_3plus_way_intersections.gdb/intersections_2018_{}_gccsa10km'.format(locale.lower()))

# Derived network data variables - no need to change, assuming the above works
network_source_feature = '{}'.format(network_source_feature_dataset)

# network dataset, without specifying the location (e.g. if gdb is work environment)
in_network_dataset = os.path.join('{}'.format(network_source_feature_dataset),
                                '{}_ND'.format(network_source_feature_dataset))
# network dataset, with full path
in_network_dataset_path = os.path.join(gdb_path,in_network_dataset)

# network
# sausage buffer network size  -- in units specified above
distance = df_parameters.loc['distance']['value']

# search tolderance (in units specified above; features outside tolerance not located when adding locations)
# NOTE: may need to increase if no locations are found
tolerance = df_parameters.loc['tolerance']['value']
 
# buffer distance for network lines as sausage buffer  
line_buffer = df_parameters.loc['line_buffer']['value']

# this distance is a limit beyond which not to search for destinations 
limit = df_parameters.loc['limit']['value']

# Threshold paramaters
soft_threshold_slope = df_parameters.loc['soft_threshold_slope']['value']

# Sausage buffer run parameters
# If you experience 'no forward edges' issues, change this value to 1
# this means that for *subsequently processed* buffers, it will use 
# an ST_SnapToGrid parameter of 0.01 instead of 0.001
## The first pass should use 0.001, however.
no_foward_edge_issues = df_studyregion.loc[locale]['no_forward_edge_issues']
snap_to_grid = 0.001
if no_foward_edge_issues == 1:
  snap_to_grid = 0.01

# POS
#  -- make sure projected according to project spatial reference

if type(df_studyregion.loc[locale]['pos_source']) is unicode:
  # implies POS has been defined; else is nan
  pos_source   = os.path.join(folderPath,df_studyregion.loc[locale]['pos_source'].encode('utf'))
  pos_vertices = df_parameters.loc['pos_vertices']['value']  # used to create series of hypothetical entry points around park

  # POS queries - combined national and state-based scenarios, as lists of query-distance 2-tuples by locale
  pos_locale = df_studyregion.loc[locale]['pos_queries'].encode('utf')

  # Feature for restricted inclusion of POS analysis; used where POS data coverage < study region extent
  # Note that at the moment we are using a single feature with a 400m Euclidean buffer; indicators
  # use network distance however (400m network would be no greater than 400 Euclidean), but some seek POS across 
  # a larger network distance of say 2km.  So, situations may arise where some edge cases are still unfairly 
  # penalised.  However in practice, for Sydney where this issue could arises the portion of study region where 
  # this issue arises is not urban and will be marked for exclusion anyway.  
  # So, to avoid a more complicated scripting approach, we are sticking with single inclusion feature, for now.
  if type(df_studyregion.loc[locale]['pos_inclusion']) is unicode:
    pos_inclusion = df_studyregion.loc[locale]['pos_inclusion'].encode('utf')
  else:
    pos_inclusion = "*"

aos_threshold = df_parameters.loc['aos_threshold']['value']
    
# Destinations - locate destinations.gdb within dest_dir (ie. 'D:\ntnl_li_2018\data\destinations\' or whereever your ntnl_li_2018 folder is located)
# Destinations data directory
dest_dir = os.path.join(folderPath,df_parameters.loc['dest_dir']['value'].encode('utf'))
src_destinations = os.path.join(dest_dir,df_parameters.loc['src_destinations']['value'].encode('utf'))
destination_id = df_parameters.loc['destination_id']['value'].encode('utf')
destinations_gdb_has_datasets = df_parameters.loc['destinations_gdb_has_datasets']['value']

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
                                      df_parameters.loc['combined_dest_template']['value'].encode('utf'))
outCombinedFeature = 'study_destinations'

# array / list of destinations 
# IMPORTANT -- These are specified in the 'destinations' worksheet of the ind_study_region_matrix.xlsx file
#               - specify: destination, domain, cutoff and count distances as required
#
#           -- If new destinations are added, they should be appended to end of list 
#              to ensure this order is respected across time.
#
# The table 'dest_type' will be created in Postgresql to keep track of destinations

df_destinations = df_destinations.replace(pandas.np.nan, 'NULL', regex=True)
destination_list = [x.encode('utf') for x in df_destinations.destination.tolist()] # the destinations 
# dest_codes = df_destinations.code.tolist()   # domain is an optional grouping category for destinations / indicators
# dest_domains = df_destinations.domain.tolist()   # domain is an optional grouping category for destinations / indicators
# dest_cutoffs = df_destinations.cutoff.tolist()   # cut off distance within which to evaluate presence
# dest_counts = df_destinations.counts.tolist()   # cut off distance within which to evaluate counts

df_osm_dest = df_osm_dest.replace(pandas.np.nan, 'NULL', regex=True)

school_destinations = df_parameters.loc['school_destinations']['value']
school_id = df_parameters.loc['school_id']['value']
school_id_type = df_parameters.loc['school_id_type']['value']

# When destinations are imported for study region, we don't want to retain all of these; now, purge
purge_dest_list = df_housekeeping.destinations_to_purge_after_import.tolist()

# specify that the above modules and all variables below are imported on 'from config.py import *'
__all__ = [x for x in dir() if x not in ['__file__','__all__', '__builtins__', '__doc__', '__name__', '__package__']]
 
