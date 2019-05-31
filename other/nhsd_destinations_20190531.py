# import modules
import os
import sys
import time
import pandas
import arcpy
import numpy as np

xls = pandas.ExcelFile('D:/ntnl_li_2018_template/process/ind_study_region_matrix.xlsx')
df_parameters = pandas.read_excel(xls, 'parameters',index_col=0)
df_parameters.value = df_parameters.value.fillna('')
for var in [x for x in df_parameters.index.values]:
    globals()[var] = df_parameters.loc[var]['value']    

df = pandas.read_excel(xls, 'nhsd classification',index_col=0)

arcpy.env.overwriteOutput = True 

arcpy.MakeFeatureLayer_management(in_features="D:/ntnl_li_2018_template/data/destinations/National_Health_Services_Directory__NHSD___Point__2017/shp/a745dd61-446b-495c-a4ca-86a2f1897a0a.shp", out_layer="nhsd")

# while the input dataset is known to be GDA94 epsg 4283 (see metadata), it was ill-defined as far as ArcGIS was concerned 
# so we have to explicitly tell ArcGIS this in order to undertake proper geographic transformation to epsg 7845
arcpy.DefineProjection_management(in_dataset="nhsd", 
                                  coor_system="GEOGCS['GCS_GDA_1994',DATUM['D_GDA_1994',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]")

for d in df.index.values if d is not np.nan:
    category = ''' "3serviceca" = '{}' '''.format(df.loc[d,'3serviceca'])
    if not df.loc[d].isnull().any():
        ## note that arcgis requires apostrophes to be replaced with double apostrophes for matching
        type = ''' AND "4servicety" = '{}' '''.format(df.loc[d,'4servicety'].replace("'","''"))
    else:
        type = ''
    layer = arcpy.SelectLayerByAttribute_management(in_layer_or_view="nhsd", 
                                            selection_type="NEW_SELECTION",
                                            where_clause='{}{}'.format(category,type))
    arcpy.Project_management(in_dataset=layer, out_dataset="D:/ntnl_li_2018_template/data/destinations/destinations.gdb/health/{}".format(d), out_coor_system=out_coor_system, transform_method=transform_method, in_coor_system="GEOGCS['GDA94',DATUM['Geocentric Datum of Australia 1994',SPHEROID['GRS 1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['degree',0.0174532925199433]]", preserve_shape="NO_PRESERVE_SHAPE", max_deviation="", vertical="NO_VERTICAL")

