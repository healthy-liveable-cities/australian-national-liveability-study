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


# Specify study region details
locale       = 'Melb'
year         = '2016'  # The year that the calculator indicator set approx. targets
region       = 'GCCSA'
region_shape = 'ABS/derived/ASGS_2016_Volume_1_GDA2020/main_GCCSA_2016_AUST_FULL.shp'
region_where_clause = ''' "STATE_NAME" = 'Victoria' AND "GCCSA_NAME"  = 'Greater Melbourne' '''

# derived study region name (no need to change!)
study_region = '{0}_{1}'.format(region,year).lower()
db = 'li_{0}_{1}'.format(locale,year).lower()

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

# TRANSFORMATIONS
#  These three variables are used for specifying a transformation from GCS GDA 1994 to GDA2020 GA LLC when using arcpy.Project_management.  Specifically, its used in the custom clipFeature function in script 02_road_network_setup.py
out_coor_system = '''PROJCS['GDA2020_GA_LCC',GEOGCS['GDA2020',DATUM['GDA2020',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',134.0],PARAMETER['Standard_Parallel_1',-18.0],PARAMETER['Standard_Parallel_2',-36.0],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]]'''

transform_method = 'GDA_1994_To_GDA2020_NTv2_CD'

in_coor_system = '''GEOGCS['GCS_GDA_1994',DATUM['D_GDA_1994',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]'''

## This is used for spatial reference for 'destinations' feature dataset (in script 07_recompile_destinations.py --- similar to out_coor_system, it contains additions bounding box parameters apparently, and a flag 'IsHighPrecision'.  
feature_ds_out_spatial_ref = '''"PROJCS['GDA2020_GA_LCC',GEOGCS['GDA2020',DATUM['GDA2020',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',134.0],PARAMETER['Standard_Parallel_1',-18.0],PARAMETER['Standard_Parallel_2',-36.0],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]];-39261800 -15381600 10000;-100000 10000;-100000 10000;0.001;0.001;0.001;IsHighPrecision"'''

# The main directory for data
folderPath = 'D:/ntnl_li_2018/ntnl_li_2018_template/data/'

# SQL Settings
db_host   = 'localhost'
db_port   = '5432'
db_user   = 'python'
db_pwd    = '***REMOVED***'

# Database names -- derived from above parameters; (no need to change!)
db = 'li_{0}_{1}'.format(locale,year).lower()
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
points          = os.path.join(folderPath,'address_points/PSMA_2018/Open GNAF/gnaf_2018_vic_GDA2020_GA_LLC.shp')
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
network_prefix                 = 'Melb'    # the prefix to road network data
network_source_gdb_suffix      = '_CleanPedestrianRoads.gdb' # the suffix to the network data source
network_source_feature_dataset = 'OSMPedClean2018' # The generalised network data name
intersections                  = 'Intersections'   # 3+ way intersections located in source network gdb
network_edges                  = '{}CleanPedRoads_2018'.format(network_prefix)
network_junctions              = '{}{}_ND_Junctions'.format(network_prefix,network_source_feature_dataset)

# Derived network data variables - no need to change, assuming the above works
network_source_gdb             = '{}{}'.format(network_prefix,network_source_gdb_suffix)
network_source_feature = '{}{}'.format(network_prefix,network_source_feature_dataset)
network_source = os.path.join(folderPath,road_data,network_source_gdb)
network = os.path.join(network_source,network_source_feature)
intersections = os.path.join(network_source, intersections)

# network dataset, without specifying the location (e.g. if gdb is work environment)
in_network_dataset = os.path.join('{}'.format(network_source_feature_dataset),
                                '{}_ND'.format(network_source_feature))
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

# POS
# POS feature sourced from R:\5050\CHE\CIV\Data\VEAC
pos_entry_src     = os.path.join(folderPath,'pos/pos.shp')
vertices_distance = 50  # used to create series of hypothetical entry points around park

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
dest_domains = data.domain.tolist()   # domain is an optional grouping category for destinations / indicators
dest_cutoffs = data.cutoff.tolist()   # cut off distance within which to evaluate presence
dest_counts = data.counts.tolist()   # cut off distance within which to evaluate counts

# specify that the above modules and all variables below are imported on 'from config.py import *'
__all__ = [x for x in dir() if x not in ['__file__','__all__', '__builtins__', '__doc__', '__name__', '__package__']]
 