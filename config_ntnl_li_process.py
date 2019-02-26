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

df_parameters.value = df_parameters.value.fillna('')

responsible = df_studyregion['responsible']

year   = df_parameters.loc['year']['value']

# The main directory for data
folderPath = df_parameters.loc['folderPath']['value']

# Set up locale (ie. defined at command line, or else testing)
if len(sys.argv) >= 2:
  locale = '{studyregion}'.format(studyregion = sys.argv[1])
else:
  locale = 'testing'
if __name__ == '__main__':
  print("\nProcessing script {} for locale {}...\n".format(sys.argv[0],locale))

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
  
# More study region details

region = df_studyregion.loc[locale]['region']
state  = df_studyregion.loc[locale]['state']

locale_dir = os.path.join(folderPath,'study_region','{}'.format(locale.lower()))

# Study region boundary
region_shape = os.path.join(folderPath,df_studyregion.loc[locale]['region_shape'])

# SQL Query to select study region
region_where_clause = df_studyregion.loc[locale]['region_where_clause']

# db suffix
suffix = df_studyregion.loc[locale]['suffix']
if suffix.dtype!='float64':
  # this implies at least one value was a string, and this can be encoded as utf
  suffix = suffix
  
if pandas.np.isnan(suffix):
  # this implies all suffixes are blank and this has been interpreted as 'nan'
  suffix = ''


# derived study region name (no need to change!)
study_region = '{0}_{1}'.format(region,year).lower()
db = 'li_{0}_{1}{2}'.format(locale,year,suffix).lower()

# ; Project spatial reference (for ArcGIS)
SpatialRef = df_parameters.loc['SpatialRef']['value']

# Project spatial reference EPSG code (for Postgis)
srid       = df_parameters.loc['srid']['value']
units      = df_parameters.loc['units']['value']
units_full = df_parameters.loc['units_full']['value']

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
temp = df_parameters.loc['temp']['value']


# location of the 'Create_Hexagon_Tessellation' user written package toolbox by Tim Whiteaker; acquired from http://www.arcgis.com/home/item.html?id=03388990d3274160afe240ac54763e57
create_hexagon_tbx = df_parameters.loc['create_hexagon_tbx']['value']
CreatePointsLines_tbx = df_parameters.loc['CreatePointsLines_tbx']['value']

# TRANSFORMATIONS
#  These three variables are used for specifying a transformation from GCS GDA 1994 to GDA2020 GA LLC when using arcpy.Project_management.  Specifically, its used in the custom clipFeature function in script 02_road_network_setup.py
out_coor_system = df_parameters.loc['out_coor_system']['value']

transform_method = df_parameters.loc['transform_method']['value']

in_coor_system = df_parameters.loc['in_coor_system']['value']

## This is used for spatial reference for 'destinations' feature dataset (in script 07_recompile_destinations.py --- similar to out_coor_system, it contains additions bounding box parameters apparently, and a flag 'IsHighPrecision'.  
feature_ds_out_spatial_ref = df_parameters.loc['feature_ds_out_spatial_ref']['value']

# SQL Settings
db_host   = df_parameters.loc['db_host']['value']
db_port   = '{}'.format(df_parameters.loc['db_port']['value'])
db_user   = df_parameters.loc['db_user']['value']
db_pwd    = df_parameters.loc['db_pwd']['value']
arc_sde_user = df_parameters.loc['arc_sde_user']['value']

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

osm_data = os.path.join(df_studyregion.loc[locale]['osm_data'])
osmconvert = df_parameters.loc['osmconvert']['value']
osm2pgsql_exe = os.path.join(folderPath,df_parameters.loc['osm2pgsql_exe']['value'])
osm2pgsql_style = os.path.join(folderPath,df_parameters.loc['osm2pgsql_style']['value'])
osm_source = df_studyregion.loc[locale]['osm_source']
osm_prefix = df_studyregion.loc[locale]['osm_prefix']

grant_query = '''GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {0};
                 GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {0};
                 GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {1};
                 GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {1};'''.format(arc_sde_user,db_user)

# Region set up
areas_of_interest = [int(x) for x in df_parameters.loc['regions_of_interest']['value'].split(',')]
areas = {}
for area in areas_of_interest + ['urban']:
  prefix = area
  if type(area) is int:
    prefix = 'region{}'.format(area)
  if df_parameters.loc['{}_data'.format(prefix)]['value'] != '':
    areas[area] = {}
    for field in ['data','name','id']:
      if field=='data':
        # join with data path prefix
        areas[area][field] = os.path.join(folderPath,df_parameters.loc['{}_{}'.format(prefix,field)]['value'])
        areas[area]['table'] = os.path.splitext(os.path.basename(areas[area]['data']))[0].lower()
      elif field=='name': 
        # split into full (f) and short (s) lower case versions; latter is safe for database use
        areas[area]['name_f'] = df_parameters.loc['{}_name'.format(prefix)]['value'].split(',')[0]
        if len(df_parameters.loc['{}_name'.format(prefix)]['value'].split(',')) > 1:
          areas[area]['name_s'] = df_parameters.loc['{}_name'.format(prefix)]['value'].split(',')[1].lower()
        else:
          areas[area]['name_s'] = areas[area]['name_f'].lower()
      else:
        areas[area][field] = df_parameters.loc['{}_{}'.format(prefix,field)]['value']

area_info = {}
for info in ['dwellings','disadvantage']:
  area_info[info] = {}
  if df_parameters.loc['{}_data'.format(info)]['value']!= '':
    area_info[info]['data']      = os.path.join(folderPath,df_parameters.loc['{}_data'.format(info)]['value'])
    area_info[info]['table']     = 'area_{}'.format(info)
    area_info[info]['area']      = int(df_parameters.loc['{}_area'.format(info)]['value'])
    area_info[info]['id']        = df_parameters.loc['{}_id'.format(info)]['value']
    area_info[info]['field']     = df_parameters.loc['{}_field'.format(info)]['value']
    area_info[info]['exclusion'] = df_parameters.loc['{}_exclusion'.format(info)]['value']

# This is a legacy configuration option not yet updated to the generic framework
# ie. this configuration is Australia specific, but must be generalised to non-specific region
# However, as of February 2019 I haven't had time to make the full update
# This configuration retained for compatability with scripts until full re-write
# Meshblock Dwellings feature name
meshblocks = areas[0]['data']
abs_SA1    = areas[1]['data']
abs_SA2    = areas[2]['data']
abs_SA3    = areas[3]['data']
abs_SA4    = areas[4]['data']
abs_lga    = areas[5]['data']
abs_suburb = areas[6]['data']
abs_SOS    = areas['urban']['data']

suburb_feature = areas[6]['table']
lga_feature = areas[5]['table']

# meshblock ID MB_CODE_20 (varname is truncated by arcgis to 8 chars) datatype is varchar(11) 
meshblock_id     = areas[0]['id']
dwellings        = area_info['dwellings']['data']
dwellings_id     = area_info['dwellings']['id']
dwellings_field  = area_info['dwellings']['field']

# parcels (point data locations used for sampling)
# Note that the process assumes we have already transformed points to the project's spatial reference
# Point data locations (e.g. GNAF address point features)
points = df_studyregion.loc[locale]['points']
points = points.split(',')
points_id = df_parameters.loc['points_id']['value']
points_srid = df_parameters.loc['points_srid']['value']

# The below is perhaps a redundant naming convention,
# but our last run of scripts invested in this, so for now we'll leave it in so things work
# A better name might be something like 'units_of_analysis' or 'included_points'
# I don't know; but that is what this refers to. Its just a name.
parcel_dwellings = 'parcel_dwellings'

# roads
# Define network data name structures
road_data = df_parameters.loc['road_data']['value']  # the folder where road data is kept
network_source = os.path.join(locale_dir,df_studyregion.loc[locale]['network_folder'])
network_source_feature_dataset = df_parameters.loc['network_source_feature_dataset']['value']
network_edges = df_parameters.loc['network_edges']['value']
network_junctions = df_parameters.loc['network_junctions']['value']
network_template = os.path.join(folderPath,road_data,df_parameters.loc['network_template']['value'])

# transformations for network (currently WGS84 to GDA GA LCC using NTv2)
network_transform_method = df_parameters.loc['network_transform_method']['value']
network_in_coor_system   = df_parameters.loc['network_in_coor_system']['value']

# Intersections with 3plus ways
clean_intersections_gpkg = df_parameters.loc['clean_intersections_gpkg']['value']
clean_intersections_locale = df_studyregion.loc[locale]['clean_intersections_locale']
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

# Threshold paramaters
soft_threshold_slope = df_parameters.loc['soft_threshold_slope']['value']

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

# Areas of Open Space
aos_threshold = df_parameters.loc['aos_threshold']['value']
    
# Destinations - locate destinations.gdb within dest_dir (ie. 'D:\ntnl_li_2018\data\destinations\' or whereever your ntnl_li_2018 folder is located)
# Destinations data directory
dest_dir = os.path.join(folderPath,df_parameters.loc['dest_dir']['value'])
src_destinations = os.path.join(dest_dir,df_parameters.loc['src_destinations']['value'])
destination_id = df_parameters.loc['destination_id']['value']
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
                                      df_parameters.loc['combined_dest_template']['value'])
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
destination_list = [x for x in df_destinations.destination.tolist()] # the destinations 
# dest_codes = df_destinations.code.tolist()   # domain is an optional grouping category for destinations / indicators
# dest_domains = df_destinations.domain.tolist()   # domain is an optional grouping category for destinations / indicators
# dest_cutoffs = df_destinations.cutoff.tolist()   # cut off distance within which to evaluate presence
# dest_counts = df_destinations.counts.tolist()   # cut off distance within which to evaluate counts

df_osm_dest = df_osm_dest.replace(pandas.np.nan, 'NULL', regex=True)

school_destinations = df_parameters.loc['school_destinations']['value']
school_id = df_parameters.loc['school_id']['value']
school_id_type = df_parameters.loc['school_id_type']['value']

# When destinations are imported for study region, we don't want to retain all of these; now, purge
purge_dest_list = [x.lower() for x in list(set(destination_list+df_housekeeping.destinations_to_purge_after_import.tolist()))]

# specify that the above modules and all variables below are imported on 'from config.py import *'
__all__ = [x for x in dir() if x not in ['__file__','__all__', '__builtins__', '__doc__', '__name__', '__package__']]
 
