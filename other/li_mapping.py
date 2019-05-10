# mapping script developed for pilot liveability project in May 2019
# - some adaption is required to apply to national liveability project
# - this script may be run from the docker ind_li environment

import os
import sys
import time
import psycopg2             # for database communication and management
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
import folium
from folium import plugins
import branca
import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement
from configparser import SafeConfigParser
from sqlalchemy import create_engine

parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# SQL Settings - storing passwords in plain text is obviously not ideal
db      = parser.get('postgresql', 'database')
db_host = "host.docker.internal"
db_user = parser.get('postgresql', 'user')
db_pwd  = parser.get('postgresql', 'password')

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db))  
                                                                      
def folium_export(locale_maps,map_name,width=1000,height=800,pause=5):
    import selenium.webdriver
    try:
        # set up options for firefox browser automation
        options=selenium.webdriver.firefox.options.Options()
        options.add_argument('--headless')
        # Profile settings to attempt to print from pdf (not currently working)
        profile = selenium.webdriver.FirefoxProfile()
        profile.set_preference("services.sync.prefs.sync.browser.download.manager.showWhenStarting", False)
        profile.set_preference("pdfjs.disabled", True)
        profile.set_preference("print.always_print_silent", True)
        profile.set_preference("print.show_print_progress", False)
        profile.set_preference("browser.download.show_plugins_in_list",False)
        # load driver with options and profile
        driver = selenium.webdriver.Firefox(firefox_profile=profile,options=options)
        driver.set_window_size(width, height)  # choose a resolution
        driver.get('file:///{}/{}/{}.html'.format(os.getcwd(),locale_maps,map_name))
        # You may need to add time.sleep(seconds) here
        time.sleep(pause)
        # Remove zoom controls from snapshot
        for leaflet_class in ["leaflet-control-zoom","leaflet-control-layers"]:
            element = driver.find_element_by_class_name(leaflet_class)
            driver.execute_script("""
            var element = arguments[0];
            element.parentNode.removeChild(element);
            """, element)
        ## PDF printing is not currently working; commented out
        # driver.execute_script("window.print();")
        driver.save_screenshot('{}/{}.png'.format(locale_maps,map_name))
        with open('{}/_{}.html'.format(locale_maps,map_name), 'w') as f:
            f.write(driver.page_source)
        driver.close()
    except FileNotFoundError as fnf_error:
        print(fnf_error)
    except AssertionError as error:
        print(error)
    except Exception as error:
        print("Export of {} failed: {}".format('{}/{}.png'.format(locale_maps,map_name),error))
        
legend_style = '''
<style>
.legend {
    padding: 0px 0px;
    font: 12px sans-serif;
    background: white;
    background: rgba(255,255,255,1);
    box-shadow: 0 0 15px rgba(0,0,0,0.2);
    border-radius: 5px;
    }
</style>
'''                                                                           


locale = 'melb_2018'
locale_maps = './../../li_analysis/maps/'
if not os.path.exists(locale_maps):
    os.makedirs(locale_maps)    
    
map_layers={}

inds = ["SA1"                                                             ,
        "Dwelling count"                                                  ,
        "Urban Liveability Index (ULI) score"                             ,
        "ULI (including air quality) score"                               ,
        "Walkability for transport"                                       ,
        "  - Daily living score (/3)"                                     ,
        "  - Dwelling density per hectare in local  neighbourhood"        ,
        "  - Intersections per km&#178; in local neighbourhood"           ,
        "Social infrastructure mix (/15)"                                 ,
        "Public transport in 400m (&#37; residential lots)"               ,
        "POS > 1.5 Ha within 400m (&#37; residential lots)"               ,
        "Predicted NO&#8322; parts per billion (Mesh Block)"              ,
        "Affordable housing (30-40 rule)"                                 ,
        "Live-work in same area (&#37;)"]                               

inds_plain = ["sa1_7dig11",
              "dwellings" ,
              "li_ci_excl_airqual" ,
              "li_ci_est" ,
              "walkability" ,
              "daily_living" ,
              "dd_nh1600m" ,
              "sc_nh1600m" ,
              "si_mix" ,
              "dest_pt" ,
              "pos15000_access" ,
              "pred_no2_2011_col_ppb" ,
              "sa1_prop_affordablehousing" ,
              "sa2_prop_live_work_sa3"]
              
polarity = ["na",
              "positive" ,
              "positive" ,
              "positive" ,
              "positive" ,
              "positive" ,
              "positive" ,
              "positive" ,
              "positive" ,
              "positive" ,
              "positive" ,
              "negative" ,
              "positive" ,
              "positive"]
              
sql = '''
SELECT                     
    a.sa1_7dig11                                     AS "{}",
    c.dwellings                                      AS "{}",
    ROUND(a.li_ci_excl_airqual         ::numeric, 1) AS "{}",
    ROUND(a.li_ci_est                  ::numeric, 1) AS "{}",
    ROUND(a.walkability                ::numeric, 1) AS "{}",
    ROUND(a.daily_living               ::numeric, 1) AS "{}",
    ROUND(a.dd_nh1600m                 ::numeric, 1) AS "{}",
    ROUND(a.sc_nh1600m                 ::numeric, 1) AS "{}",
    ROUND(a.si_mix                     ::numeric, 1) AS "{}",
    ROUND(100*a.dest_pt                ::numeric, 1) AS "{}",
    ROUND(100*a.pos15000_access        ::numeric, 1) AS "{}",
    ROUND(a.pred_no2_2011_col_ppb      ::numeric, 1) AS "{}",
    ROUND(a.sa1_prop_affordablehousing ::numeric, 1) AS "{}",
    ROUND(100*a.sa2_prop_live_work_sa3 ::numeric, 1) AS "{}",
    ST_Transform(b.geom,4326) geom
  FROM li_raw_soft_sa1_7dig11 a
LEFT JOIN sa1_2011_aust b ON a.sa1_7dig11 = b.sa1_7dig11::int
LEFT JOIN (SELECT sa1_7dig11,
                  SUM(dwellings) AS dwellings 
             FROM abs_linkage GROUP BY sa1_7dig11) c 
ON a.sa1_7dig11 = c.sa1_7dig11
'''.format(*inds)
map_layers['sa1'] = gpd.GeoDataFrame.from_postgis(sql, engine, geom_col='geom' )

sql = '''
SELECT                     
    'study region'::text,
    ST_Transform(ST_Union(b.geom),4326) geom
  FROM li_raw_soft_sa1_7dig11 a
LEFT JOIN sa1_2011_aust b
ON a.sa1_7dig11 = b.sa1_7dig11::int
'''.format(*inds)
map_layers['region'] = gpd.GeoDataFrame.from_postgis(sql, engine, geom_col='geom' )

xy = [float(map_layers['region'].centroid.y),float(map_layers['region'].centroid.x)]  
bounds = map_layers['region'].bounds.transpose().to_dict()[0]

print("\nPlease inspect results using interactive maps saved in project maps folder:")
# for indicator in inds[1:-1]:

# add layers
folium_for_web = True
for current_ind in range(3,len(inds)):
    if polarity[current_ind] == 'positive':
        m = folium.Map(location=xy, zoom_start=11,tiles=None, control_scale=True, prefer_canvas=True, zoom_control = folium_for_web)
        m.add_tile_layer(tiles='Stamen Toner',
                        name='Basemap (Stamen Toner)', 
                        overlay=True,
                        attr=(
                            "Map tiles: <a href=\"http://stamen.com/\">Stamen Design</a>," 
                            "under <a href=\"http://creativecommons.org/licenses/by/3.0\">CC BY 3.0</a>, featuring " 
                            "data by <a href=\"http://openstreetmap.org/\">OpenStreetMap</a>,"
                            "under <a href=\"http://creativecommons.org/licenses/by-sa/3.0\">CC BY SA</a>.")
                        )
        ind_name = '{}, by {}'.format(inds[current_ind],inds[0])
        legend_text = '{} quartiles, by {}'.format(inds[current_ind],inds[0])
        bins = list(map_layers['sa1'][inds[current_ind]].quantile([0, 0.25, 0.5, 0.75, 1]))  
        liveability = folium.Choropleth(data=map_layers['sa1'],
                        geo_data =map_layers['sa1'].to_json(),
                        name = ind_name,
                        columns =[inds[0],inds[current_ind]],
                        key_on="feature.properties.{}".format(inds[0]),
                        fill_color='BrBG',
                        fill_opacity=0.7,
                        line_opacity=0.1,
                        legend_name=legend_text,
                        bins = bins,
                        reset=True,
                        overlay = True
                        ).add_to(m)
        folium.features.GeoJsonTooltip(fields=inds,
                                    labels=True, 
                                    sticky=True
                                    ).add_to(liveability.geojson)
        if folium_for_web:
            folium.LayerControl(collapsed=True).add_to(m)
        m.fit_bounds(m.get_bounds())
        m.get_root().html.add_child(folium.Element(legend_style))
        # save map
        map_name = '{}_{}_{}'.format(locale,inds_plain[current_ind],inds[0].lower())
        m.save('{}/{}.html'.format(locale_maps,map_name))
        # folium_export(locale_maps,map_name)
        print("\t- {}".format(map_name))  
    else:
        # polarity is negative
        m = folium.Map(location=xy, zoom_start=11,tiles=None, control_scale=True, prefer_canvas=True, zoom_control = folium_for_web)
        m.add_tile_layer(tiles='Stamen Toner',
                        name='Basemap (Stamen Toner)', 
                        overlay=True,
                        attr=(
                            "Map tiles: <a href=\"http://stamen.com/\">Stamen Design</a>," 
                            "under <a href=\"http://creativecommons.org/licenses/by/3.0\">CC BY 3.0</a>, featuring " 
                            "data by <a href=\"http://openstreetmap.org/\">OpenStreetMap</a>,"
                            "under <a href=\"http://creativecommons.org/licenses/by-sa/3.0\">CC BY SA</a>.")
                        )
        ind_name = '{}, by {}'.format(inds[current_ind],inds[0])
        legend_text = '{} quartiles, by {}'.format(inds[current_ind],inds[0])
        bins = list(map_layers['sa1'][inds[current_ind]].quantile([0, 0.25, 0.5, 0.75, 1]))  
        liveability = folium.Choropleth(data=map_layers['sa1'],
                        geo_data =map_layers['sa1'].to_json(),
                        name = ind_name,
                        columns =[inds[0],inds[current_ind]],
                        key_on="feature.properties.{}".format(inds[0]),
                        fill_color='YlOrBr',
                        fill_opacity=0.7,
                        line_opacity=0.1,
                        legend_name=legend_text,
                        bins = bins,
                        reset=True,
                        overlay = True
                        ).add_to(m)
        folium.features.GeoJsonTooltip(fields=inds,
                                    labels=True, 
                                    sticky=True
                                    ).add_to(liveability.geojson)  
        if folium_for_web:
            folium.LayerControl(collapsed=True).add_to(m)
        m.fit_bounds(m.get_bounds())
        m.get_root().html.add_child(folium.Element(legend_style))
        # save map
        map_name = '{}_{}_{}'.format(locale,inds_plain[current_ind],inds[0].lower())
        m.save('{}/{}.html'.format(locale_maps,map_name))
        # folium_export(locale_maps,map_name)
        print("\t- {}".format(map_name))  
        
# for current_ind in range(3,len(inds)):
    # map_name = '{}_{}_{}'.format(locale,inds_plain[current_ind],inds[0].lower())
    # folium_export(locale_maps,map_name)