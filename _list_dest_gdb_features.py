import arcpy
import os

# Import custom variables for National Liveability indicator process
from _project_setup import *


arcpy.env.workspace = dest_gdb

datasets = arcpy.ListDatasets(feature_type='feature')
datasets = [''] + datasets if datasets is not None else []

for ds in datasets:
    for fc in arcpy.ListFeatureClasses(feature_dataset=ds):
        path = os.path.join(arcpy.env.workspace, ds, fc)
        print(path)