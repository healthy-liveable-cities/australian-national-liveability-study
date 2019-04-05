# Output sql to gpkg 

for locale in ['adelaide','darwin','hobart','melb']:
  c1 = '''psql -U postgres -c  "CREATE DATABASE li_{}_2016;"'''.format(locale)
  c2 = '''psql -U postgres -d li_{}_2016 -c   "CREATE EXTENSION postgis;SELECT postgis_full_version();"'''.format(locale)
  c3 = '''psql li_{0}_2016 < D:/ntnl_li_2018_template/data/study_region/wgs84_epsg4326/map_features/li_{0}_2016.sql postgres'''.format(locale)
  c4 = '''ogr2ogr -overwrite -f GPKG D:/ntnl_li_2018_template/data/study_region/wgs84_epsg4326/map_features/li_map_li_{0}_2016.gpkg PG:"host=localhost user=python dbname=li_{0}_2016 password=***REMOVED***"  "li_map_sa1" "li_map_ssc" "li_map_lga" "ind_description" "boundaries_sa1" "boundaries_ssc" "boundaries_lga" "urban_sos"'''.format(locale)
  print("\n{}:".format(locale))
  print(c1)
  print(c2)
  print(c3)
  print(c4)
  