# Script:  config_ntnl_li_process.py
# Liveability indicator calculation template custom configuration file
# Version: 20180531
# Author:  Carl Higgs
# About:
# All scripts within the process folder draw on the sources, parameters and modules
# specified in this file to source and output resources.  As such,
# it is the best definition of where resources are sourced from and should be import os
# modified within a local implementation of the liveability index.

# Directions:
# - customise entries here to match your 
    # - data sources 
    # - folder structure
    # - project parameters
# - Once customised: 
#   **** Save this file (00_4_setup_config_ntnl_li_process.py) 
#   **** as 'config_ntnl_li_process.py' within the 'process' 
#
#   **** Remember to update config_destionations.csv
#   **** Specify in config_destionations.csv: destinations, domain, cutoff, count distance

# import modules
import os
import sys
import pandas

# The main directory for data
folderPath = 'D:/ntnl_li_2018_template/data/'

# Specify study region details  ************** This is the important bit! *******
# Uncomment for study region as required

# locale       = 'Adelaide'
# locale       = 'Bris'
# locale       = 'Canberra'
# locale       = 'Darwin'
# locale       = 'Hobart'
# locale       = 'Melb'
# locale       = 'Perth'
# locale       = 'Syd'

# SQL Query to select study region
region_where_clause_list = {'Adelaide': ''' "STATE_NAME" = 'South Australia' AND "GCCSA_NAME"  = 'Greater Adelaide' ''',
                            'Bris': ''' "STATE_NAME" = 'Queensland' AND "GCCSA_NAME"  = 'Greater Brisbane' ''',
                            'Canberra': ''' "STATE_NAME" = 'Australian Capital Territory' AND "GCCSA_NAME"  = 'Australian Capital Territory' ''',
                            'Darwin': ''' "STATE_NAME" = 'Northern Territory' AND "GCCSA_NAME"  = 'Greater Darwin' ''',
                            'Hobart': ''' "STATE_NAME" = 'Tasmania' AND "GCCSA_NAME"  = 'Greater Hobart' ''',
                            'Melb': ''' "STATE_NAME" = 'Victoria' AND "GCCSA_NAME"  = 'Greater Melbourne' ''',
                            'Perth': ''' "STATE_NAME" = 'Western Australia' AND "GCCSA_NAME"  = 'Greater Perth' ''',
                            'Syd': ''' "STATE_NAME" = 'New South Wales' AND "GCCSA_NAME"  = 'Greater Sydney' '''}
region_where_clause = region_where_clause_list[locale]

# Point data locations (ie. GNAF address point features, in GDA2020 GA LCC)
points_list = {'Adelaide': os.path.join(folderPath,'address_points/GDA2020_GA_LCC.gdb/gnaf_2018_sa') ,
               'Bris'    : os.path.join(folderPath,'address_points/GDA2020_GA_LCC.gdb/gnaf_2018_qld'),
               'Canberra': os.path.join(folderPath,'address_points/GDA2020_GA_LCC.gdb/gnaf_2018_act'),
               'Darwin'  : os.path.join(folderPath,'address_points/GDA2020_GA_LCC.gdb/gnaf_2018_nt') ,
               'Hobart'  : os.path.join(folderPath,'address_points/GDA2020_GA_LCC.gdb/gnaf_2018_tas'),
               'Melb'    : os.path.join(folderPath,'address_points/GDA2020_GA_LCC.gdb/gnaf_2018_vic'),
               'Perth'   : os.path.join(folderPath,'address_points/GDA2020_GA_LCC.gdb/gnaf_2018_wa') ,
               'Syd'     : os.path.join(folderPath,'address_points/GDA2020_GA_LCC.gdb/gnaf_2018_nsw')}

points = points_list[locale]

# POS queries - combined national and state-based scenarios, as lists of query-distance 2-tuples by locale
pos_queries = {'Bris':  [['',400],
                         ['area_ha > 1.5',400],
                         ['area_ha > 0.5',400],
                         ['area_ha > 5',2500]],
               'Melb':  [['',400],
                         ['area_ha > 1.5',400]],
               'Perth': [['',400],
                         ['area_ha > 1.5',400],
                         ['area_ha > 0.4 AND area_ha <= 1 ',400],
                         ['area_ha > 1 AND area_ha <= 5',800],
                         ['area_ha > 5 AND area_ha <= 20',2000],
                         ['',300]],
               'Syd':   [['',400],
                         ['area_ha > 1.5',400],
                         ['area_ha > 0.5',400],
                         ['area_ha > 2',2000]]}
if locale in pos_queries:
  pos_locale = pos_queries[locale]
  
# Intersections data locations (ie. 3+ way intersection clipped to 10km buffered GCCSA, in GDA2020 GA LCC)                     
intersections = os.path.join(folderPath,'roads/GDA2020_GA_LCC_3plus_way_intersections.gdb/gnaf_2018_intersections_2018_{}_gccsa10km'.format(locale.lower()))
  
# **************** Extra bits (hopefully don't need to change much) *******************************

# More study region details
year         = '2016'  # The year that the calculator indicator set approx. targets
region       = 'GCCSA'
region_shape = 'ABS/derived/ASGS_2016_Volume_1_GDA2020/main_GCCSA_2016_AUST_FULL.shp'

# db suffix
suffix = ''

# derived study region name (no need to change!)
study_region = '{0}_{1}'.format(region,year).lower()
db = 'li_{0}_{1}{2}'.format(locale,year,suffix).lower()

# ; Project spatial reference (for ArcGIS)
SpatialRef = 'GDA2020 GA LCC'

# Project spatial reference EPSG code (for Postgis)
srid = 7845
units = 'm'
units_full = 'metres'

# Study region buffer
study_buffer = 10000
buffered_study_region = '{0}_{1}{2}'.format(study_region,study_buffer,units)

# hexagon diagonal length and buffer distance (metres)
#   -- hexagon sides will be half the length of this value
#   -- hexagon area is 3/2 * sqrt(3) * (hex_diag/2)^2
#  so with diag of 3000 m, area is 5845671.476 sq.m.
hex_diag   = 3000
hex_buffer = 3000

# Derived hex settings - no need to change
hex_grid = '{0}_hex_{1}{2}_diag'.format(study_region,hex_diag,units)
hex_grid_buffer =  '{0}_hex_{1}{2}_diag_{3}{2}_buffer'.format(study_region,hex_diag,units,hex_buffer)
hex_side = float(hex_diag)*0.5

# Temp folder  - be aware that some files may be created and stored here
#   - if issues arise you may need to manually remove such files 
temp = 'C:/temp'


# location of the 'Create_Hexagon_Tessellation' user written package toolbox
#   -- by Tim Whiteaker
#   -- acquired from http://www.arcgis.com/home/item.html?id=03388990d3274160afe240ac54763e57
create_hexagon_tbx = '../process/arcgis_packages/Create_Hexagon_Tessellation_41BC0CF7-3B1F-4598-8DE6-D5EE78060052/v101/Create_Hexagon_Tessellation.tbx'
CreatePointsLines_tbx = '../process/arcgis_packages/CreatePointsLines.tbx'

# TRANSFORMATIONS
#  These three variables are used for specifying a transformation from GCS GDA 1994 to GDA2020 GA LLC when using arcpy.Project_management.  Specifically, its used in the custom clipFeature function in script 02_road_network_setup.py
out_coor_system = '''PROJCS['GDA2020_GA_LCC',GEOGCS['GDA2020',DATUM['GDA2020',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',134.0],PARAMETER['Standard_Parallel_1',-18.0],PARAMETER['Standard_Parallel_2',-36.0],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]]'''

transform_method = 'GDA_1994_To_GDA2020_NTv2_CD'

in_coor_system = '''PROJCS['GDA_1994_VICGRID94',GEOGCS['GCS_GDA_1994',DATUM['D_GDA_1994',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',2500000.0],PARAMETER['False_Northing',2500000.0],PARAMETER['Central_Meridian',145.0],PARAMETER['Standard_Parallel_1',-36.0],PARAMETER['Standard_Parallel_2',-38.0],PARAMETER['Latitude_Of_Origin',-37.0],UNIT['Meter',1.0]]'''

## This is used for spatial reference for 'destinations' feature dataset (in script 07_recompile_destinations.py --- similar to out_coor_system, it contains additions bounding box parameters apparently, and a flag 'IsHighPrecision'.  
feature_ds_out_spatial_ref = '''"PROJCS['GDA2020_GA_LCC',GEOGCS['GDA2020',DATUM['GDA2020',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',134.0],PARAMETER['Standard_Parallel_1',-18.0],PARAMETER['Standard_Parallel_2',-36.0],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]];-39261800 -15381600 10000;-100000 10000;-100000 10000;0.001;0.001;0.001;IsHighPrecision"'''

# SQL Settings
db_host   = 'localhost'
db_port   = '5432'
db_user   = 'python'
db_pwd    = '***REMOVED***'

# Database names -- derived from above parameters; (no need to change!)
gdb       = '{}.gdb'.format(db)
db_sde    = '{}.sde'.format(db)
gdb_path    = os.path.join(folderPath,gdb)
db_sde_path = os.path.join(folderPath,db_sde)
dbComment = 'Liveability indicator data for {0} {1}.'.format(locale,year)
arc_sde_user = 'arc_sde'

os.environ['PGHOST']     = db_host
os.environ['PGPORT']     = db_port
os.environ['PGUSER']     = db_user
os.environ['PGPASSWORD'] = db_pwd
os.environ['PGDATABASE'] = db

grant_query = '''GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {0};
                 GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {0};
                 GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {1};
                 GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {1};'''.format(arc_sde_user,db_user)

# ABS data
# ABS data sourced online is located in the ABS_downloads folder
# cleaned data referenced below (including output from scripts) is located in ABS_derived

# Index of Relative Socio-Economic Disadvantage (cleaned)
abs_irsd  = os.path.join(folderPath,'ABS/ABS_derived/ABS_2011_IRSD.csv')
              
# meshblock source shape file (ABS download)
# Meshblock Dwellings feature name
meshblocks   = os.path.join(folderPath,'ABS/derived/ASGS_2016_Volume_1_GDA2020/main_MB_2016_AUST_FULL.shp')

# meshblock ID MB_CODE_20 (varname is truncated by arcgis to 8 chars) datatype is varchar(11) 
meshblock_id    = 'MB_CODE_20'
final_meshblock_id = 'mb_code_2016'

# Dwelling count source csv (ABS download)
# CLEAN APPLIED: removed comments from end of file
dwellings        = os.path.join(folderPath,'ABS/derived/2016_census_mesh_block_counts.csv')
dwellings_id     = 'MB_CODE_2016'
dwellings_field   = 'Dwelling'

# other areas
abs_SA1    = os.path.join(folderPath,'ABS/derived/ASGS_2016_Volume_1_GDA2020/main_SA1_2016_AUST_FULL.shp')
abs_SA2    = os.path.join(folderPath,'ABS/derived/ASGS_2016_Volume_1_GDA2020/main_SA2_2016_AUST_FULL.shp')
abs_SA3    = os.path.join(folderPath,'ABS/derived/ASGS_2016_Volume_1_GDA2020/main_SA3_2016_AUST_FULL.shp')
abs_SA4    = os.path.join(folderPath,'ABS/derived/ASGS_2016_Volume_1_GDA2020/main_SA4_2016_AUST_FULL.shp')
abs_lga    = os.path.join(folderPath,'ABS/derived/ASGS_2016_Volume_3_GDA2020/main_LGA_2016_AUST.shp')
abs_suburb = os.path.join(folderPath,'ABS/derived/ASGS_2016_Volume_3_GDA2020/main_SSC_2016_AUST.shp')

# parcels
# Note that the process assumes we have already transformed points to GDA2020 GA LLC

points_id        = 'gnaf_pid'
points_srid    = 7844

# The below is perhaps a redundant naming convention,
# but our last run of scripts invested in this, so for now we'll leave it in so things work
# A better name might be something like 'units_of_analysis' or 'included_points'
# I don't know; but that is what this refers to. Its just a name.
parcel_dwellings = 'parcel_dwellings'

# roads
# Define network data name structures
road_data                      = 'roads'   # the folder where road data is kept
network_source = os.path.join(folderPath,road_data,'osm_gccsa10km_{}_pedestrian_20180628'.format(locale.lower()))
network_source_feature_dataset = 'PedestrianRoads' # The generalised network data name
network_edges = 'edges'
network_junctions = 'nodes'
network_template = os.path.join(folderPath,road_data,'osmnx_nd_template.xml')

# transformations for network (currently WGS84 to GDA GA LCC using NTv2)
network_transform_method = ''' GDA_1994_To_WGS_1984 + GDA_1994_To_GDA2020_NTv2_CD''' 
network_in_coor_system = ''' GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]'''

intersections = 'Intersections'   # 3+ way intersections located in source network gdb

# Derived network data variables - no need to change, assuming the above works
network_source_feature = '{}'.format(network_source_feature_dataset)
# network = os.path.join(network_source,network_source_feature)
intersections = os.path.join('road_data','intersections', intersections)

# network dataset, without specifying the location (e.g. if gdb is work environment)
in_network_dataset = os.path.join('{}'.format(network_source_feature_dataset),
                                '{}_ND'.format(network_source_feature_dataset))
# network dataset, with full path
in_network_dataset_path = os.path.join(gdb_path,in_network_dataset)

# network
# sausage buffer network size  -- in units specified above
distance = 1600

# search tolderance (in units specified above; features outside tolerance not located when adding locations)
# NOTE: may need to increase if no locations are found
tolerance = 500
 
# buffer distance for network lines as sausage buffer  
line_buffer = 50

# this distance is a limit beyond which not to search for destinations 
limit = 3000

# Threshold paramaters
soft_threshold_slope = 5

# POS
# POS feature sourced from Julianna Rozek
#  -- already projected in GDA2020 GA LCC
#  -- various sources (OSM, Government, ??)
pos_source   = os.path.join(folderPath,'destinations','pos_2018.gdb','{}_pos_2018'.format(locale.lower()))
pos_vertices = 50  # used to create series of hypothetical entry points around park

# Destinations - locate destinations.gdb within dest_dir (ie. 'D:\ntnl_li_2018\data\destinations\' or whereever your ntnl_li_2018 folder is located)
# Destinations data directory
dest_dir = os.path.join(folderPath,'destinations')
src_destinations = os.path.join(dest_dir,'destinations.gdb')
destination_id = 'dest_oid'
destinations_gdb_has_datasets = 'FALSE'

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
                                      'destinations/combined_dest_template/combined_dest_template.shp')
outCombinedFeature = 'study_destinations'

# array / list of destinations 
# IMPORTANT -- These must be specified in the config_destinations.csv file
#               - specify: destination, domain, cutoff and count distances as required
#
#           -- If new destinations are added, they should be appended to end of list 
#              to ensure this order is respected across time.
#
# The table 'dest_type' will be created in Postgresql to keep track of destinations

## Read in the externally defined csv file
data = pandas.read_csv(os.path.join(sys.path[0],'config_destinations.csv'))
data = data.replace(pandas.np.nan, 'NULL', regex=True)

## Retrieve defined variables from destinations csv
destination_list = data.destination.tolist() # the destinations 
dest_codes = data.code.tolist()      # code for destination type (regardless of name, which may be study region specific)
dest_domains = data.domain.tolist()  # domain is an optional grouping category for destinations / indicators
dest_cutoffs = data.cutoff.tolist()  # cut off distance within which to evaluate presence
dest_counts = data.counts.tolist()   # cut off distance within which to evaluate counts

# specify that the above modules and all variables below are imported on 'from config.py import *'
__all__ = [x for x in dir() if x not in ['__file__','__all__', '__builtins__', '__doc__', '__name__', '__package__']]
 