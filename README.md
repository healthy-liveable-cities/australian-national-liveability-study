# National Liveability Indicator project

Healthy Liveable Cities, RMIT University 2020

## About

This repository contains scripts to process liveability indicators for cities in Australia.

The project commenced as a framework for calculating national liveability indicators targetting 2016 for each capital city, building on the scripted workflow developed for the Liveability Index project (Higgs et al. 2019).  The code was expanded in 2018-19 for calculating indicators targetting 2018 for Australia's 21 largest cities.  This work was undertaken to meet requests for indicator data products from a number of other projects, including the Australian Early Development of Census linkage, and The National Cities Performance Framework (Public transport and public open space indicators).  

In 2020 a refresh of the codebase was undertaken to update code to work with Python 3, ArcGIS Pro, using a Conda environment, and an updated database schema.  As of 1 June 2020 this work is still in development.

## Organisation ##

* 'admin'
** contains miscellanious documents relativing to system set up and liveability indicator construction

* 'data'
** contains the folder structure to contain source and intermediary processed data files
** Note that files are not stored in the bitbucket repository, although some files may be
** stored in the network drive copy of the repository (e.g. ABS files)

*  'maps'
** This folder could be used to store arcgis or qgis map documents and their pdf outputs

* 'process'
** A series of indexed scripts and directions used to create the National Liveability Indicators.


The main project directory also contains a loosely structured document containing notes on use of PostgreSQL (e.g. for queries, import and export of data, etc).  This may be of value to others wishing to e.g. interface R with Postgresql, run similar SQL queries, backup the SQL database or restore a previous backup.  

## Installation

The mid-2020 update is designed to be run on a Windows computer with an authorised installation of ArcGIS Pro (which allows use of Python 3, the current version of Python as of 2020).  In addition to installing ArcGIS Pro, you also need to download and install the 'ArcGIS Coordinate Systems Data for ArcGIS Pro Per User Install' (locate in Data and Content from my.esri.com); this is required for using the GDA2020 NTv2 transformation grid (e.g. for EPSG 7845).  Also ensure authorisation is obtained for using the ArcGIS Pro Network Analyst extension.

Once ArcGIS Pro has been set up and authorised, you must clone the default Conda environment.  Conda refers to the Anaconda python package manager, and is used to set up a replicable working environment, incorporating the arcpy libraries.  The below instructions assume default path locations, however some customisation may be required for your set up.

As of 1 June 2020, the below directions are a draft of the process, with code refactoring a work in progress.

1. Clone conda environment

`C:\Program Files\ArcGIS\Pro\bin\Python\Scripts>conda create --name li_2020 --clone arcgispro-py3`

2. Create a copy of proenv.txt, with a name like 'proenv.txt.backup', and change the path in the original to 'C:\Program Files\ArcGIS\Pro\bin\Python\envs\li_2020', after confirming that0 this folder exists

3. Activate conda environment
`D:\ntnl_li_2018_template\process>"C:\Program Files\ArcGIS\Pro\bin\Python\Scripts\activate.bat" l
i_2020`

4. Install additional packages
`(li_2020) D:\ntnl_li_2018_template\process>"C:\Program Files\ArcGIS\Pro\bin\Python\Scripts\conda" install osmnx python-igraph psycopg2 geoalchemy2 rasterio folium sphinx xlrd selenium seaborn contextily rasterstats pandana  pyproj psutil`

A draft process has been created for running the above four steps, assuming default installation location, by running
`_setup_li_2020.bat`

Then, the li_2020 analysis environment can be run by typing,
`_li_2020.bat`

at the command prompt.  (NB. as of 2 June 2020 this is a draft process and the setup script in particular has not been fully verified).


### Contact ###

* Carl Higgs <carlhiggs@rmit.edu.au>

Carl Higgs 1 June 2020