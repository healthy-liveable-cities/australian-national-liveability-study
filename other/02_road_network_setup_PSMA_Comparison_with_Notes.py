# Purpose: Set up road network for study region
# Author:  Carl Higgs
# Date:    20180627
#

# The PSMA road network from 2013 (the one used in Liveability Index)
# D:/liveability/data/Roads/CLEAN_Roads_2013.gdb
# was 
# Projected to GDA2020 GA LCC
 # --  manually clipped to the 10km Melbourne GCCSA

## The above was all done interactively / in the ArcMap Python interpreter using the below code (in part based on the 2017 national indicator scripts)
      
arcpy.Project_management(in_dataset="D:/ntnl_li_2018_template/data/roads/CLEAN_Roads_2013.gdb/PedestrianRoads", out_dataset="C:/Users/Carl/Documents/ArcGIS/Default.gdb/PedestrianRoads_Project", out_coor_system="PROJCS['GDA2020_GA_LCC',GEOGCS['GDA2020',DATUM['GDA2020',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',134.0],PARAMETER['Standard_Parallel_1',-18.0],PARAMETER['Standard_Parallel_2',-36.0],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]]", transform_method="GDA_1994_To_GDA2020_NTv2_CD", in_coor_system="PROJCS['GDA_1994_VICGRID94',GEOGCS['GCS_GDA_1994',DATUM['D_GDA_1994',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',2500000.0],PARAMETER['False_Northing',2500000.0],PARAMETER['Central_Meridian',145.0],PARAMETER['Standard_Parallel_1',-36.0],PARAMETER['Standard_Parallel_2',-38.0],PARAMETER['Latitude_Of_Origin',-37.0],UNIT['Meter',1.0]]", preserve_shape="NO_PRESERVE_SHAPE", max_deviation="", vertical="NO_VERTICAL")

outputNetwork = "PedestrianRoads"
SpatialRef = arcpy.SpatialReference('GDA2020 GA LCC')
arcpy.Delete_management(arcpy.env.scratchGDB)

arcpy.CreateFeatureDataset_management(arcpy.env.scratchGDB,outputNetwork, spatial_reference = SpatialRef)

inputNetwork = os.path.join('C:/Users/Carl/Documents/ArcGIS/Default.gdb','PedestrianRoads_Project')
arcpy.env.workspace = inputNetwork

clippingFeature = "D:/ntnl_li_2018_template/data/li_melb_2016_psma.gdb/gccsa_2016_10000m"

features = arcpy.ListFeatureClasses()

arcpy.env.overwriteOutput = True 
for fc in features:
  print(fc)
  print(os.path.join(inputNetwork,fc))
  arcpy.MakeFeatureLayer_management(fc, 'feature') 
  arcpy.SelectLayerByLocation_management('feature', 'intersect',clippingFeature)
  arcpy.CopyFeatures_management('feature', os.path.join(arcpy.env.scratchGDB,outputNetwork,fc))
  
# RoadsCLEAN
# C:/Users/Carl/Documents/ArcGIS/Default.gdb\PedestrianRoads_Project\RoadsCLEAN
# RoundaboutsCLEAN
# C:/Users/Carl/Documents/ArcGIS/Default.gdb\PedestrianRoads_Project\RoundaboutsCLEAN
# Intersections
# C:/Users/Carl/Documents/ArcGIS/Default.gdb\PedestrianRoads_Project\Intersections
# RoadEnds
# C:/Users/Carl/Documents/ArcGIS/Default.gdb\PedestrianRoads_Project\RoadEnds
# PedestrianRoads_ND_Junctions
# C:/Users/Carl/Documents/ArcGIS/Default.gdb\PedestrianRoads_Project\PedestrianRoads_ND_Junctions



# -- A network dataset was created using Arc Catalog using these edges and junctions
# -- This was built, then copied into the li_melb_2016_psma dataset
      