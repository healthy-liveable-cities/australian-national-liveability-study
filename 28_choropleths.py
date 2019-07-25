# Script:  _choropleths.py
# Purpose: Diagnostic chorpleth maps
# Author:  Carl Higgs
# Date:    20190521

import os
import sys
import pandas
import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement
import folium
from folium import plugins
import branca
from sqlalchemy import create_engine
import psycopg2
import numpy as np
import json

# Import custom variables for National Liveability indicator process
# Load settings from ind_study_region_matrix.xlsx
xls = pandas.ExcelFile('./ind_study_region_matrix.xlsx')
df_about = pandas.read_excel(xls, 'about')
print(df_about.iloc[0].reset_index()['index'].to_string(index=False).encode('utf'))
df_parameters = pandas.read_excel(xls, 'parameters',index_col=0)
for var in [x for x in df_parameters.index.values]:
    globals()[var] = df_parameters.loc[var]['value']   
df_studyregion = pandas.read_excel(xls, 'study_regions',index_col=1)
responsible = df_studyregion['responsible']
year   = df_parameters.loc['year']['value']
# SQL Settings
db_host   = df_parameters.loc['db_host']['value']
db_port   = '{}'.format(df_parameters.loc['db_port']['value'])
db_user   = df_parameters.loc['db_user']['value']
db_pwd    = df_parameters.loc['db_pwd']['value']
db_host = "host.docker.internal"
who = sys.argv[1]
print('Analyst: {}\n'.format(who))
locales = responsible[responsible == who].sort_values().index.values.tolist()

def folium_to_png(input_dir='',output_dir='',map_name='',width=1000,height=800,pause=3):
    import selenium.webdriver
    import time
    try:
        if (input_dir=='' or map_name==''):
            raise Exception(('This function requires specification of an input directory.\n'
                'Please specify the function in the following form:\n'
                'folium_to_png(intput_dir,output_dir,map_name,[width],[height],[pause])'
                ))
            return
        if output_dir=='':
            output_dir = input_dir
        options=selenium.webdriver.firefox.options.Options()
        options.add_argument('--headless')
        driver = selenium.webdriver.Firefox(options=options)
        driver.set_window_size(width, height)  # choose a resolution
        driver.get('file:///{}/{}/{}.html'.format(os.getcwd(),input_dir,map_name))
        # You may need to add time.sleep(seconds) here
        time.sleep(pause)
        # Remove zoom controls from snapshot
        for leaflet_class in ["leaflet-control-zoom","leaflet-control-layers"]:
            element = driver.find_element_by_class_name(leaflet_class)
            driver.execute_script("""
            var element = arguments[0];
            element.parentNode.removeChild(element);
            """, element)
        driver.save_screenshot('{}/{}.png'.format(output_dir,map_name))
        driver.close()
    except Exception as error:
        print("Export of {} failed.".format('{}/{}.png: {}'.format(output_dir,map_name,error)))
        
map_style = '''
<style>
.legend {
    padding: 0px 0px;
    font: 12px sans-serif;
    background: white;
    background: rgba(255,255,255,1);
    box-shadow: 0 0 15px rgba(0,0,0,0.2);
    border-radius: 5px;
    white-space: normal;
    width:120%;
    }
#legend {
    width:120%;
}
.leaflet-control-attribution {
    width: 60%;
    height: auto;
    }
</style>
'''   


# The shape file for map features are output 
map_features_outpath = os.path.join(folderPath,'study_region','wgs84_epsg4326','map_features')

if not os.path.exists(map_features_outpath):
  os.makedirs(map_features_outpath)   

for locale in locales:
    full_locale = df_studyregion.loc[locale]['full_locale']
    print(full_locale)
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
    study_region = '{0}_{1}'.format(region,year).lower()
    db = 'li_{0}_{1}{2}'.format(locale,year,suffix).lower()
    
    engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                        pwd  = db_pwd,
                                                                        host = db_host,
                                                                        db   = db))                                                            
    
    # Prepare map
    locale_maps = '../maps/{}_{}'.format(locale,year)
    if not os.path.exists(locale_maps):
        os.makedirs(locale_maps)    
    for dir in ['html','png','pdf','gpkg']:
        path = os.path.join(locale_maps,dir)
        if not os.path.exists(path):
            os.makedirs(path)   
    
    map_layers={}
    
    map_layers['boundary'] = gpd.GeoDataFrame.from_postgis('''SELECT ST_Transform(geom,4326) geom FROM {}'''.format(study_region), engine, geom_col='geom')
    
    sql = '''
    SELECT a.*,
        ST_Transform(b.geom,4326) geom
    FROM li_inds_sa1 a
    LEFT JOIN main_sa1_2016_aust_full b ON a.sa1_maincode = b.sa1_mainco
    '''
    map_layers['data'] = gpd.GeoDataFrame.from_postgis(sql, engine, geom_col='geom')
    
    # get and format the indicator matrix for this study region
    ind_description = pandas.read_sql_query('''SELECT indicators,agg_form,units,unit_level_description FROM ind_summary''', 
                                            index_col = 'indicators',
                                            con=engine)  
    
    xy = [float(map_layers['boundary'].centroid.y),float(map_layers['boundary'].centroid.x)]  
    bounds = map_layers['boundary'].bounds.transpose().to_dict()[0]
    
    indicators = [ind for ind in map_layers['data'].columns.values if ind not in ['sa1_maincode','geom']]
    for indicator in indicators:
      if indicator!='sample_point_count':    
        # replacing apostrophe with unicode apostrophe to ensure html code for legend
        # isn't corrupted by prematurely closed string (as may occur if this isn't done)
        description = ind_description.loc[indicator,'unit_level_description'].replace(u"'",u'’')
        agg_form = ind_description.loc[indicator,'agg_form']
        units = '   ({})'.format(ind_description.loc[indicator,'units'])
        if '(None)' in units:
            units = ''
        title = '{}: SA1, {}{}'.format(description,agg_form,units)
        print('  {}'.format(title))
        if all(i is None for i in map_layers['data'][indicator].values):
            print("\t- all records are null for this indicator")
        else:
            map_layers['data'][indicator] = map_layers['data'][indicator].round(2)
            # Population raster map (includes the raster overlay)
            m = folium.Map(location=xy, zoom_start=11,tiles=None, control_scale=True, prefer_canvas=True)
            folium.TileLayer(tiles='Stamen Toner',
                            name='simple map', 
                            active=True,
                            attr=((
                                " {} | "
                                "Map tiles: <a href=\"http://stamen.com/\">Stamen Design</a>, " 
                                "under <a href=\"http://creativecommons.org/licenses/by/3.0\">CC BY 3.0</a>, featuring " 
                                "data by <a href=\"https://wiki.osmfoundation.org/wiki/Licence/\">OpenStreetMap</a>, "
                                "under ODbL.").format(map_attribution))
                                    ).add_to(m)
                                    
            # bins = list(map_layers['data']['population'].quantile([0, 0.25, 0.5, 0.75, 1]))                             
            data_layer = folium.Choropleth(data=map_layers['data'][['sa1_maincode',indicator,'geom']],
                            geo_data =map_layers['data'][['sa1_maincode',indicator,'geom']].to_json(),
                            name = description,
                            columns =['sa1_maincode',indicator],
                            key_on="feature.properties.sa1_maincode",
                            fill_color='YlGn',
                            fill_opacity=0.7,
                            line_color='#FFFFFF',
                            line_opacity=0.2,
                            legend_name=title,
                            # bins=bins,
                            reset=True,
                            overlay = True
                            ).add_to(m)
                                        
            folium.features.GeoJsonTooltip(fields=['sa1_maincode', indicator,],
                                        labels=True, 
                                        sticky=True
                                        ).add_to(data_layer.geojson)                          
                                                            
            folium.LayerControl(collapsed=True).add_to(m)
            m.fit_bounds(m.get_bounds())
            m.get_root().html.add_child(folium.Element(map_style))
        
            # save map
            map_name = '{}_sa1_{}'.format(locale,indicator)
            if not os.path.isfile('{}/html/{}.html'.format(locale_maps,map_name)):
                m.save('{}/html/{}.html'.format(locale_maps,map_name))
                print("\t- {}.html".format(map_name))
            else:
                print("\t- {}.html exists".format(map_name))
            if not os.path.isfile(os.path.join(locale_maps,'png','{}.png'.format(map_name))):
                folium_to_png(os.path.join(locale_maps,'html'),os.path.join(locale_maps,'png'),map_name)
                print("\t- {}.png".format(map_name)) 
            else:
                print("\t- {}.png exists".format(map_name))
