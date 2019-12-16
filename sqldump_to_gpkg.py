# Output sql to gpkg 

for locale in ['perth','melbourne','sydney']:
  command = '''ogr2ogr -overwrite -f GPKG D:/highlife/indicators/data/study_region/{locale}/for_gdb_features_{locale}.gpkg PG:"host=localhost user=python dbname=hl_{locale}_2019 password=***REMOVED***"  "aos_nodes_30m_line" "study_destinations"'''.format(locale = locale)
  # print("\n{}:".format(locale))
  print(command)
  