"C:\Program Files\ArcGIS\Pro\bin\Python\Scripts\conda" create --name li_2020 --clone arcgispro-py3
"C:\Program Files\ArcGIS\Pro\bin\Python\Scripts\activate.bat" li_2020
"C:\Program Files\ArcGIS\Pro\bin\Python\Scripts\conda" install osmnx python-igraph psycopg2 geoalchemy2 rasterio folium sphinx xlrd selenium seaborn contextily rasterstats pandana tqdm pyproj psutil
