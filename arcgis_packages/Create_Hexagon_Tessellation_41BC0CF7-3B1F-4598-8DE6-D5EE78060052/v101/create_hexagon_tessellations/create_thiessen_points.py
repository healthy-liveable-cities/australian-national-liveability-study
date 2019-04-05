#!/usr/bin/env python
"""Creates points spaced such that Thiessen polygons will be hexagons.

ArcGIS Version:  10
Author:  Tim Whiteaker
Remarks:
    It's best if a projected coordinate system is used.  
    
"""

import math
import os

import arcpy


def create_thiessen_points(study_area, side_length, output_fc):
    """Creates points spaced such that Thiessen polygons will be hexagons.

    Arguments:
        study_area -- feature class defining area of interest
        side_length -- length of regular hexagon side
        output_fc -- name and location of output feature class

    Remarks:
        Hexagons can be created for Thiessen polygons built from points spaced
        in a pattern like the one below.

        *   *   *   *
          *   *   *
        *   *   *   *
          *   *   *
        *   *   *   *

    """
    
    # Validate inputs
    count = int(str(arcpy.GetCount_management(study_area)))
    if count == 0:
        arcpy.AddError('Error: No features found in ' + str(study_area))
        return
    side_length = float(side_length)
    if side_length <= 0:
        arcpy.AddError('Error: Hexagon side length must be greater than zero.')
        return

    # Determine point spacing
    dx = 3.0 * side_length
    dy = side_length / 2.0 * math.sqrt(3.0)
    indent = dx / 2

    # Get the extent of the study area.
    # If in ArcMap, make sure we use feature coordinates, not map coordinates.
    desc = arcpy.Describe(study_area)
    if desc.dataType == "FeatureLayer":
        desc = arcpy.Describe(desc.featureClass.catalogPath)
    ext = desc.extent

    # Determine number of rows and columns.  Add extra just to be sure.
    xmin = ext.XMin - dx
    ymin = ext.YMin - dy * 3.0
    xmax = ext.XMax + dx
    ymax = ext.YMax + dy * 3.0
    num_rows = int((ymax - ymin) / dy) + 1
    num_cols = int((xmax - xmin) / dx) + 2

    # Create the output feature class
    spatial_ref = desc.spatialReference
    workspace = os.path.dirname(output_fc)
    fc_name = os.path.basename(output_fc)
    fc = arcpy.CreateFeatureclass_management(
        workspace, fc_name, "POINT", "", "", "", spatial_ref)

    # Populate output features
    arcpy.AddMessage('Creating ' + str(num_rows * num_cols) + ' points...')
    cursor = arcpy.InsertCursor(output_fc)
    feature = None

    try:
        y = ymin
        for r in range(num_rows):
            x = xmin - indent / 2
            if r % 2 != 0:
                x += indent

            for c in range(num_cols):
                feature = cursor.newRow()
                p = arcpy.Point()
                p.X = x
                p.Y = y
                feature.shape = p
                cursor.insertRow(feature)
                x += dx

            y += dy
                
    finally:
        if feature:
            del feature
        if cursor:
            del cursor


if __name__ == '__main__':
    is_test = False

    if is_test:
        raise Exception(' Testing not yet implemented')
    else:
        study_area = arcpy.GetParameterAsText(0)
        side_length = arcpy.GetParameterAsText(1)
        output_fc = arcpy.GetParameterAsText(2)

    create_thiessen_points(study_area, side_length, output_fc)

